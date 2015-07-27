package org.ihtsdo.otf.query.lucene;

import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.Get;
import gov.vha.isaac.ochre.api.chronicle.ObjectChronology;
import gov.vha.isaac.ochre.api.component.sememe.SememeChronology;
import gov.vha.isaac.ochre.api.component.sememe.SememeType;
import gov.vha.isaac.ochre.api.component.sememe.version.DescriptionSememe;
import gov.vha.isaac.ochre.api.component.sememe.version.DynamicSememe;
import gov.vha.isaac.ochre.api.component.sememe.version.SememeVersion;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.queryparser.classic.ParseException;
import org.glassfish.hk2.runlevel.RunLevel;
import org.ihtsdo.otf.tcc.api.blueprint.ComponentProperty;
import org.ihtsdo.otf.tcc.api.description.DescriptionChronicleBI;
import org.ihtsdo.otf.tcc.api.description.DescriptionVersionBI;
import org.ihtsdo.otf.tcc.api.refexDynamic.RefexDynamicChronicleBI;
import org.ihtsdo.otf.tcc.api.refexDynamic.RefexDynamicVersionBI;
import org.ihtsdo.otf.tcc.model.index.service.IndexerBI;
import gov.vha.isaac.ochre.api.index.SearchResult;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.document.IntField;
import org.jvnet.hk2.annotations.Service;

/**
 * Lucene Manager for a Description index. Provides the description indexing
 * service.
 *
 * This has been redesigned such that is now creates multiple columns within the
 * index
 *
 * There is a 'everything' column, which gets all descriptions, to support the
 * standard search where you want to match on a text value anywhere it appears.
 *
 * There are 3 columns to support FSN / Synonym / Definition - to support
 * searching that subset of descriptions. There are also data-defined columns to
 * support extended definition types - for example - loinc description types -
 * to support searching terminology specific fields.
 *
 * Each of the columns above is also x2, as everything is indexed both with a
 * standard analyzer, and with a whitespace analyzer.
 *
 * @author aimeefurber
 * @author <a href="mailto:daniel.armbrust.list@gmail.com">Dan Armbrust</a>
 */
@Service(name = "Description indexer")
@RunLevel(value = 2)
public class LuceneDescriptionIndexer extends LuceneIndexer implements IndexerBI {

    private static final Logger logger = Logger.getLogger(LuceneDescriptionIndexer.class.getName());

    private static final Semaphore setupNidsSemaphore = new Semaphore(1);
    private static final AtomicBoolean nidsSetup = new AtomicBoolean(false);

    private final HashMap<Integer, String> nidTypeMap = new HashMap<>();
    private int descSourceTypeNid;

    // for HK2 only
    private LuceneDescriptionIndexer() throws IOException {
        super("descriptions");
    }

    @Override
    protected boolean indexChronicle(ObjectChronology<?> chronicle) {
        setupNidConstants();
        if (chronicle instanceof DescriptionChronicleBI) {
            return true;
        }
        if (chronicle instanceof SememeChronology) {
            SememeChronology<?> sememeChronology = (SememeChronology) chronicle;
            if (sememeChronology.getSememeType() == SememeType.DESCRIPTION) {
                return true;
            }
        }

        return false;
    }

    @Override
    protected void addFields(ObjectChronology<?> chronicle, Document doc) {
        if (chronicle instanceof DescriptionChronicleBI) {
            indexOtfDescription(doc, chronicle);
        } else if (chronicle instanceof SememeChronology) {
            SememeChronology<?> sememeChronology = (SememeChronology) chronicle;
            if (sememeChronology.getSememeType() == SememeType.DESCRIPTION) {
                indexOchreDescription(doc, (SememeChronology<DescriptionSememe>) sememeChronology);
            }
        }
    }

    private void indexOtfDescription(Document doc, ObjectChronology<?> chronicle) {
        doc.add(new IntField(ComponentProperty.COMPONENT_ID.name(), chronicle.getNid(), LuceneIndexer.indexedComponentNidType));
        DescriptionChronicleBI desc = (DescriptionChronicleBI) chronicle;
        String lastDescText = null;
        String lastDescType = null;

        TreeMap<Long, String> uniqueTextValues = new TreeMap<>();

        for (DescriptionVersionBI<?> descriptionVersion : desc.getVersionList()) {
            String descType = nidTypeMap.get(descriptionVersion.getTypeNid());

            //No need to index if the text is the same as the previous version.
            if ((lastDescText == null) || (lastDescType == null)
                    || !lastDescText.equals(descriptionVersion.getText())
                    || !lastDescType.equals(descType)) {
                //Add to the field that carries all text
                addField(doc, ComponentProperty.DESCRIPTION_TEXT.name(), descriptionVersion.getText());

                //Add to the field that carries type-only text
                addField(doc, ComponentProperty.DESCRIPTION_TEXT.name() + "_" + descType, descriptionVersion.getText());

                uniqueTextValues.put(descriptionVersion.getTime(), descriptionVersion.getText());
                lastDescText = descriptionVersion.getText();
                lastDescType = descType;
            }
        }

        try {
            //index the extended description types - matching the text values and times above with the times of these annotations.
            String lastExtendedDescType = null;
            String lastValue = null;
            for (RefexDynamicChronicleBI<?> rdc : desc.getRefexDynamicAnnotations()) {
                for (RefexDynamicVersionBI<?> rdv : rdc.getVersionList()) {
                    if (Get.taxonomyService().wasEverKindOf(rdv.getAssemblageNid(), descSourceTypeNid)) {
                        //this is a UUID, but we want to treat it as a string anyway
                        String extendedDescType = rdv.getData()[0].getDataObject().toString();
                        String value = null;

                        //Find the text that was active at the time of this refex - timestamp on the refex must not be
                        //greater than the timestamp on the value
                        for (Entry<Long, String> x : uniqueTextValues.entrySet()) {
                            if (value == null || x.getKey() <= rdv.getTime()) {
                                value = x.getValue();
                            } else if (x.getKey() > rdv.getTime()) {
                                break;
                            }
                        }

                        if (lastExtendedDescType == null || lastValue == null
                                || !lastExtendedDescType.equals(extendedDescType)
                                || !lastValue.equals(value)) {
                            if (extendedDescType == null || value == null) {
                                throw new RuntimeException("design failure");
                            }
                            addField(doc, ComponentProperty.DESCRIPTION_TEXT.name() + "_" + extendedDescType, value);
                            lastValue = value;
                            lastExtendedDescType = extendedDescType;
                        }
                    }
                }
            }
        } catch (IOException | RuntimeException e) {
            logger.log(Level.WARNING, "Failure Indexing Extended Description Type", e);
        }
    }

    private void addField(Document doc, String fieldName, String value) {
        //index twice per field - once with the standard analyzer, once with the whitespace analyzer.
        doc.add(new TextField(fieldName, value, Field.Store.NO));
        doc.add(new TextField(fieldName + PerFieldAnalyzer.WHITE_SPACE_FIELD_MARKER, value, Field.Store.NO));
    }

    /**
     * Search All Description types
     *
     * @param query The query to apply.
     * @param sizeLimit The maximum size of the result list.
     * @param targetGeneration target generation that must be included in the
     * search or Long.MIN_VALUE if there is no need to wait for a target
     * generation. Long.MAX_VALUE can be passed in to force this query to wait
     * until any in progress indexing operations are completed - and then use
     * the latest index.
     * @param prefixSearch if true, utilize a search algorithm that is optimized
     * for prefix searching, such as the searching that would be done to
     * implement a type-ahead style search. Does not use the Lucene Query
     * parser. Every term (or token) that is part of the query string will be
     * required to be found in the result.
     *
     * Note, it is useful to NOT trim the text of the query before it is sent in
     * - if the last word of the query has a space character following it, that
     * word will be required as a complete term. If the last word of the query
     * does not have a space character following it, that word will be required
     * as a prefix match only.
     *
     * For example: The query "family test" will return results that contain
     * 'Family Testudinidae' The query "family test " will not match on
     * 'Testudinidae', so that will be excluded.
     *
     *
     * @return a List of <code>SearchResult</codes> that contains the nid of the
     * component that matched, and the score of that match relative to other
     * matches.
     * @throws NumberFormatException
     * @throws IOException
     * @throws ParseException
     */
    public final List<SearchResult> query(String query, boolean prefixSearch, int sizeLimit, Long targetGeneration) {
        return query(query, prefixSearch, ComponentProperty.DESCRIPTION_TEXT, sizeLimit, targetGeneration);
    }

    /**
     * Search the specified description type.
     *
     * @param query The query to apply
     * @param extendedDescriptionType - The UUID of an extended description type
     * - should be a child of the concept "Description name in source
     * terminology (foundation metadata concept)" If this is passed in as null,
     * this falls back to a standard description search that searches all
     * description types
     * @param sizeLimit The maximum size of the result list.
     * @param targetGeneration target generation that must be included in the
     * search or Long.MIN_VALUE if there is no need to wait for a target
     * generation. Long.MAX_VALUE can be passed in to force this query to wait
     * until any in progress indexing operations are completed - and then use
     * the latest index.
     * @return a List of <code>SearchResult</codes> that contains the nid of the
     * component that matched, and the score of that match relative to other
     * matches.
     * @throws NumberFormatException
    */
    public final List<SearchResult> query(String query, UUID extendedDescriptionType, int sizeLimit, Long targetGeneration) {

        if (extendedDescriptionType == null) {
            return query(query, false, sizeLimit, targetGeneration);
        } else {
            try {
                return search(buildTokenizedStringQuery(query, ComponentProperty.DESCRIPTION_TEXT.name() + "_" + extendedDescriptionType.toString(), false),
                        sizeLimit, targetGeneration);
            } catch (IOException | ParseException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    /**
     * Search the specified description type.
     *
     * @param query The query to apply
     * @param descriptionType - The type of description to search. If this is
     * passed in as null, this falls back to a standard description search that
     * searches all description types
     * @param sizeLimit The maximum size of the result list.
     * @param targetGeneration target generation that must be included in the
     * search or Long.MIN_VALUE if there is no need to wait for a target
     * generation. Long.MAX_VALUE can be passed in to force this query to wait
     * until any in progress indexing operations are completed - and then use
     * the latest index.
     * @return a List of <code>SearchResult</codes> that contains the nid of the
     * component that matched, and the score of that match relative to other
     * matches.
     * @throws NumberFormatException
     */
    public final List<SearchResult> query(String query, LuceneDescriptionType descriptionType, int sizeLimit, Long targetGeneration) {
        if (descriptionType == null) {
            return query(query, false, sizeLimit, targetGeneration);
        } else {
            try {
                return search(buildTokenizedStringQuery(query, ComponentProperty.DESCRIPTION_TEXT.name() + "_" + descriptionType.name(), false),
                        sizeLimit, targetGeneration);
            } catch (IOException | ParseException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @PostConstruct
    private void startMe() throws IOException {
        logger.info("Starting LuceneDescriptionIndexer post-construct");
    }

    private void setupNidConstants() {
        // Can't put these in the start me, becuase if the database is not yet imported, then 
        // these calls will fail. 
        if (!nidsSetup.get()) {
            setupNidsSemaphore.acquireUninterruptibly();
            try {
                if (!nidsSetup.get()) {
                    nidTypeMap.put(IsaacMetadataAuxiliaryBinding.FULLY_SPECIFIED_NAME.getNid(), LuceneDescriptionType.FSN.name());
                    nidTypeMap.put(IsaacMetadataAuxiliaryBinding.DEFINITION_DESCRIPTION_TYPE.getNid(), LuceneDescriptionType.DEFINITION.name());
                    nidTypeMap.put(IsaacMetadataAuxiliaryBinding.SYNONYM.getNid(), LuceneDescriptionType.SYNONYM.name());
                    // add sequences also. 
                    nidTypeMap.put(IsaacMetadataAuxiliaryBinding.FULLY_SPECIFIED_NAME.getConceptSequence(), LuceneDescriptionType.FSN.name());
                    nidTypeMap.put(IsaacMetadataAuxiliaryBinding.DEFINITION_DESCRIPTION_TYPE.getConceptSequence(), LuceneDescriptionType.DEFINITION.name());
                    nidTypeMap.put(IsaacMetadataAuxiliaryBinding.SYNONYM.getConceptSequence(), LuceneDescriptionType.SYNONYM.name());
                    descSourceTypeNid = IsaacMetadataAuxiliaryBinding.DESCRIPTION_SOURCE_TYPE_REFERENCE_SETS.getNid();
                }
                nidsSetup.set(true);
            } finally {
                setupNidsSemaphore.release();
            }
        }
    }

    //TODO dan hacking Keith question - if are these required, or not?  Can we put this in the parent class?  Or should we make abstract?
    //shouldn't be a thing we have to remember to implement.
    @PreDestroy
    private void stopMe() throws IOException {
        logger.info("Stopping LuceneDescriptionIndexer pre-destroy. ");
        commitWriter();
        closeWriter();
    }

    private void indexOchreDescription(Document doc, SememeChronology<DescriptionSememe> sememeChronology) {
        doc.add(new IntField(ComponentProperty.COMPONENT_ID.name(), sememeChronology.getNid(), LuceneIndexer.indexedComponentNidType));
        String lastDescText = null;
        String lastDescType = null;

        TreeMap<Long, String> uniqueTextValues = new TreeMap<>();

        for (DescriptionSememe descriptionVersion : sememeChronology.getVersionList()) {
            String descType = nidTypeMap.get(descriptionVersion.getDescriptionTypeConceptSequence());

            //No need to index if the text is the same as the previous version.
            if ((lastDescText == null) || (lastDescType == null)
                    || !lastDescText.equals(descriptionVersion.getText())
                    || !lastDescType.equals(descType)) {
                //Add to the field that carries all text
                addField(doc, ComponentProperty.DESCRIPTION_TEXT.name(), descriptionVersion.getText());

                //Add to the field that carries type-only text
                addField(doc, ComponentProperty.DESCRIPTION_TEXT.name() + "_" + descType, descriptionVersion.getText());

                uniqueTextValues.put(descriptionVersion.getTime(), descriptionVersion.getText());
                lastDescText = descriptionVersion.getText();
                lastDescType = descType;
            }
        }

        //index the extended description types - matching the text values and times above with the times of these annotations.
        String lastExtendedDescType = null;
        String lastValue = null;
        for (SememeChronology<? extends SememeVersion> sememeChronicle : sememeChronology.getSememeList()) {
            if (sememeChronicle.getSememeType() == SememeType.DYNAMIC) {
                SememeChronology<DynamicSememe> sememeDynamicChronicle = (SememeChronology<DynamicSememe>) sememeChronicle;
                for (DynamicSememe sememeDynamic : sememeDynamicChronicle.getVersionList()) {
                    if (Get.taxonomyService().wasEverKindOf(sememeDynamic.getAssemblageSequence(), descSourceTypeNid)) {
                        //this is a UUID, but we want to treat it as a string anyway
                        String extendedDescType = sememeDynamic.getData()[0].getDataObject().toString();
                        String value = null;

                        //Find the text that was active at the time of this refex - timestamp on the refex must not be
                        //greater than the timestamp on the value
                        for (Entry<Long, String> x : uniqueTextValues.entrySet()) {
                            if (value == null || x.getKey() <= sememeDynamic.getTime()) {
                                value = x.getValue();
                            } else if (x.getKey() > sememeDynamic.getTime()) {
                                break;
                            }
                        }

                        if (lastExtendedDescType == null || lastValue == null
                                || !lastExtendedDescType.equals(extendedDescType)
                                || !lastValue.equals(value)) {
                            if (extendedDescType == null || value == null) {
                                throw new RuntimeException("design failure");
                            }
                            addField(doc, ComponentProperty.DESCRIPTION_TEXT.name() + "_" + extendedDescType, value);
                            lastValue = value;
                            lastExtendedDescType = extendedDescType;
                        }
                    }
                }
            }
        }
    }
}
