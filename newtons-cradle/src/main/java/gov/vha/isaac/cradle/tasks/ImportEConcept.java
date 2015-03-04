package gov.vha.isaac.cradle.tasks;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.cradle.waitfree.CasSequenceObjectMap;
import gov.vha.isaac.cradle.component.ConceptChronicleDataEager;
import gov.vha.isaac.cradle.taxonomy.DestinationOriginRecord;
import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordPrimitive;
import gov.vha.isaac.cradle.taxonomy.TaxonomyFlags;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.SequenceProvider;
import java.io.IOException;

import org.ihtsdo.otf.tcc.dto.TtkConceptChronicle;
import org.ihtsdo.otf.tcc.model.cc.concept.ConceptChronicle;

import java.util.UUID;
import java.util.concurrent.*;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.ihtsdo.otf.tcc.model.cc.relationship.Relationship;
import org.ihtsdo.otf.tcc.model.cc.relationship.RelationshipVersion;

/**
 * Created by kec on 7/20/14.
 */
public class ImportEConcept implements Callable<Void> {

    private static final SequenceProvider sequenceProvider = Hk2Looker.getService(SequenceProvider.class);
    private static final CradleExtensions cradle = Hk2Looker.getService(CradleExtensions.class);
    ;
    
    private static final int isaNid;

    private static final int statedNid;
    private static final int inferredNid;
    public static CasSequenceObjectMap<TaxonomyRecordPrimitive> originDestinationTaxonomyRecords;
    public static ConcurrentSkipListSet<DestinationOriginRecord> destinationOriginRecordSet;

    static {
        try {
            originDestinationTaxonomyRecords = cradle.getOriginDestinationTaxonomyMap();
            destinationOriginRecordSet = cradle.getDestinationOriginRecordSet();
            isaNid = cradle.getNidForUuids(IsaacMetadataAuxiliaryBinding.IS_A.getUuids());
            statedNid = cradle.getNidForUuids(IsaacMetadataAuxiliaryBinding.STATED.getUuids());
            inferredNid = cradle.getNidForUuids(IsaacMetadataAuxiliaryBinding.INFERRED.getUuids());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    TtkConceptChronicle eConcept;
    Semaphore permit;
    UUID newPathUuid = null;

    public ImportEConcept(TtkConceptChronicle eConcept,
            Semaphore permit, UUID newPathUuid) {
        this(eConcept, permit);
        this.newPathUuid = newPathUuid;
    }

    public ImportEConcept(TtkConceptChronicle eConcept,
            Semaphore permit) {
        this.eConcept = eConcept;
        this.permit = permit;
    }

    @Override
    public Void call() throws Exception {
        try {
            if (this.newPathUuid != null) {
                eConcept.processComponentRevisions(r -> r.setPathUuid(newPathUuid));
            }

            int conceptNid = cradle.getNidForUuids(eConcept.getPrimordialUuid());
            cradle.setConceptNidForNid(conceptNid, conceptNid);
            ConceptChronicle cc = ConceptChronicle.get(conceptNid);
            ConceptChronicle.mergeWithEConcept(eConcept, cc);
            ConceptChronicleDataEager conceptData = (ConceptChronicleDataEager) cc.getData();
            int originSequence = sequenceProvider.getConceptSequence(cc.getNid());

            TaxonomyRecordPrimitive parentTaxonomyRecord;
            if (originDestinationTaxonomyRecords.containsKey(originSequence)) {
                parentTaxonomyRecord = originDestinationTaxonomyRecords.get(originSequence).get();
            } else {
                parentTaxonomyRecord = new TaxonomyRecordPrimitive();
            }

            for (Relationship rel : conceptData.getSourceRels()) {
                int destinationSequence
                        = sequenceProvider.getConceptSequence(rel.getDestinationNid());
                assert destinationSequence != originSequence;
                rel.getVersions().stream().forEach((rv) -> {
                    if (isaRelType(rv)) {
                        int parentRecordFlags = TaxonomyFlags.PARENT.bits;
                        if (statedRelType(rv)) {
                            parentRecordFlags += TaxonomyFlags.STATED.bits;
                        } else if (inferredRelType(rv)) {
                            parentRecordFlags += TaxonomyFlags.INFERRED.bits;
                        }
                        parentTaxonomyRecord.getTaxonomyRecordUnpacked()
                                .addStampRecord(destinationSequence, rv.getStamp(), parentRecordFlags);
                    } else {
                        int parentRecordFlags = TaxonomyFlags.OTHER_CONCEPT.bits;
                        if (statedRelType(rv)) {
                            parentRecordFlags += TaxonomyFlags.STATED.bits;
                        } else if (inferredRelType(rv)) {
                            parentRecordFlags += TaxonomyFlags.INFERRED.bits;
                        }
                        parentTaxonomyRecord.getTaxonomyRecordUnpacked().addStampRecord(destinationSequence, rv.getStamp(), parentRecordFlags);
                    }
                });
                destinationOriginRecordSet.add(new DestinationOriginRecord(destinationSequence, originSequence));
            }
            originDestinationTaxonomyRecords.put(originSequence, parentTaxonomyRecord);
            cradle.writeConceptData(conceptData);
            return null;
        } finally {
            permit.release();
        }
    }

    private static boolean inferredRelType(RelationshipVersion rv) {
        return rv.getCharacteristicNid() == inferredNid;
    }

    private static boolean statedRelType(RelationshipVersion rv) {
        return rv.getCharacteristicNid() == statedNid;
    }

    private static boolean isaRelType(RelationshipVersion rv) {
        return rv.getTypeNid() == isaNid;
    }

}
