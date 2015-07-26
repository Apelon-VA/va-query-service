/*
 * Copyright 2013 International Health Terminology Standards Development Organisation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ihtsdo.otf.query.implementation;

import gov.vha.isaac.metadata.coordinates.LanguageCoordinates;
import gov.vha.isaac.metadata.coordinates.LogicCoordinates;
import gov.vha.isaac.metadata.coordinates.StampCoordinates;
import gov.vha.isaac.metadata.coordinates.ViewCoordinates;
import gov.vha.isaac.ochre.api.Get;
import gov.vha.isaac.ochre.api.chronicle.LatestVersion;
import gov.vha.isaac.ochre.api.component.concept.ConceptChronology;
import gov.vha.isaac.ochre.api.component.concept.ConceptVersion;
import gov.vha.isaac.ochre.api.component.sememe.version.DescriptionSememe;
import gov.vha.isaac.ochre.api.coordinate.LanguageCoordinate;
import gov.vha.isaac.ochre.api.coordinate.LogicCoordinate;
import gov.vha.isaac.ochre.api.coordinate.PremiseType;
import gov.vha.isaac.ochre.api.coordinate.StampCoordinate;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import gov.vha.isaac.ochre.collections.NidSet;
import gov.vha.isaac.ochre.collections.SememeSequenceSet;
import gov.vha.isaac.ochre.model.coordinate.TaxonomyCoordinateImpl;
import org.ihtsdo.otf.query.implementation.clauses.*;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.ddo.ComponentReference;
import org.ihtsdo.otf.tcc.ddo.concept.ConceptChronicleDdo;
import org.ihtsdo.otf.tcc.ddo.concept.component.description.DescriptionChronicleDdo;
import org.ihtsdo.otf.tcc.ddo.concept.component.description.DescriptionVersionDdo;
import org.ihtsdo.otf.tcc.ddo.fetchpolicy.RefexPolicy;
import org.ihtsdo.otf.tcc.ddo.fetchpolicy.RelationshipPolicy;
import javax.xml.bind.annotation.*;
import java.io.IOException;
import java.util.*;
import org.ihtsdo.otf.tcc.api.store.Ts;

/**
 * Executes queries within the terminology hierarchy and returns the nids of the
 * components that match the criterion of query.
 *
 * @author kec
 */
@XmlRootElement(name = "query")
@XmlAccessorType(value = XmlAccessType.NONE)
@XmlType(factoryClass = QueryFactory.class,
        factoryMethod = "createQuery")

public abstract class Query {

    @XmlElementWrapper(name = "for")
    @XmlElement(name = "component")
    protected List<ComponentCollectionTypes> forCollectionTypes = new ArrayList<>();

    @XmlElementWrapper(name = "custom-for")
    @XmlElement(name = "uuid")
    protected Set<UUID> customCollection = new HashSet<>();

    public static final String currentViewCoordinateKey = "Current view coordinate";
    @XmlElementWrapper(name = "let")
    private HashMap<String, Object> letDeclarations;

    @XmlElementWrapper(name = "where")
    @XmlElement(name = "clause")
    protected Clause[] rootClause = new Clause[1];

    @XmlElementWrapper(name = "return")
    @XmlElement(name = "type")
    private final EnumSet<ReturnTypes> returnTypes = EnumSet.of(ReturnTypes.NIDS);

    public void setup() {
        getLetDeclarations();
        rootClause[0] = Where();
        ForSetSpecification forSetSpec = ForSetSpecification();
        forCollectionTypes = forSetSpec.getForCollectionTypes();
        customCollection = forSetSpec.getCustomCollection();
    }

    protected abstract ForSetSpecification ForSetSpecification();

    public HashMap<String, Object> getLetDeclarations() {
        if (letDeclarations == null) {
            letDeclarations = new HashMap<>();
            if (!letDeclarations.containsKey(currentViewCoordinateKey)) {
                if (stampCoordinate != null) {
                    letDeclarations.put(currentViewCoordinateKey, stampCoordinate);
                } else {
                    letDeclarations.put(currentViewCoordinateKey, ViewCoordinates.getDevelopmentInferredLatestActiveOnly());
                }
            }

            Let();
        }
        return letDeclarations;
    }
    /**
     * Number of Components output in the returnResultSet method.
     */
    int resultSetLimit = 50;

    /**
     * The concepts, stored as nids in a <code>NidSet</code>, that are
     * considered in the query.
     */
    private NidSet forSet;
    /**
     * The steps required to compute the query clause.
     */
    private final EnumSet<ClauseComputeType> computeTypes
            = EnumSet.noneOf(ClauseComputeType.class);
    /**
     * The <code>StampCoordinate</code> used in the query.
     */
    private StampCoordinate stampCoordinate;

    /**
     * The <code>LanguageCoordinate</code> used in the query.
     */
    private LanguageCoordinate languageCoordinate;

    private PremiseType premiseType = PremiseType.INFERRED;

    private LogicCoordinate logicCoordinate = LogicCoordinates.getStandardElProfile();

    /**
     * Retrieves what type of iterations are required to compute the clause.
     *
     * @return an <code>EnumSet</code> of the compute types required
     */
    public EnumSet<ClauseComputeType> getComputePhases() {
        return computeTypes;
    }

    /**
     * No argument constructor, which creates a <code>Query</code> with the
     * Snomed inferred latest as the input <code>ViewCoordinate</code>.
     */
    public Query() {
        this(null, null);
    }

    /**
     * Constructor for <code>Query</code>. If a <code>ViewCoordinate</code> is
     * not specified, the default is the Snomed inferred latest.
     *
     * @param stampCoordinate
     */
    public Query(StampCoordinate stampCoordinate) {
        this(stampCoordinate, null);
    }

    public Query(StampCoordinate stampCoordinate, LanguageCoordinate languageCoordinate) {
        if (stampCoordinate == null) {
            this.stampCoordinate = StampCoordinates.getDevelopmentLatestActiveOnly();
        } else {
            this.stampCoordinate = stampCoordinate;
        }
        if (languageCoordinate == null) {
            this.languageCoordinate = LanguageCoordinates.getUsEnglishLanguagePreferredTermCoordinate();
        } else {
            this.languageCoordinate = languageCoordinate;
        }
    }

    /**
     * Determines the set that will be searched in the query.
     *
     * @return the <code>NativeIdSetBI</code> of the set that will be queried
     */
    protected final NidSet For() {
        forSet = new NidSet();
        for (ComponentCollectionTypes collection : forCollectionTypes) {
            switch (collection) {
                case ALL_COMPONENTS:
                    forSet.or(NidSet.ofAllComponentNids());
                    break;
                case ALL_CONCEPTS:
                    forSet.or(NidSet.of(ConceptSequenceSet.ofAllConceptSequences()));
                    break;
                case ALL_DESCRIPTION:
                    forSet.or(NidSet.ofAllComponentNids()); //TODO change to description assemblage after OTF removed
                    break;
                case ALL_RELATIONSHIPS:
                    forSet.or(NidSet.ofAllComponentNids());//TODO change to logic graph sememes after OTF removed
                    break;
                case ALL_SEMEMES:
                    forSet.or(NidSet.of(SememeSequenceSet.ofAllSememeSequences()));
                    break;
                case CUSTOM_SET:
                    customCollection.stream().forEach((uuid) -> {
                        forSet.add(Ts.get().getNidForUuids(uuid));
                    });
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
        return forSet;
    }

    public abstract void Let();

    /**
     * Retrieves the root clause of the query.
     *
     * @return root <code>Clause</code> in the query
     */
    public abstract Clause Where();

    public void let(String key, Object object) {
        letDeclarations.put(key, object);
    }

    /**
     * Constructs the query and computes the set of concepts that match the
     * criterion specified in the clauses.
     *
     * @return the <code>NativeIdSetBI</code> of nids that meet the criterion of
     * the query
     */
    public NidSet compute() {
        setup();
        forSet = For();
        getLetDeclarations();
        rootClause[0] = Where();
        NidSet possibleComponents
                = rootClause[0].computePossibleComponents(forSet);
        if (computeTypes.contains(ClauseComputeType.ITERATION)) {
            NidSet conceptsToIterateOver = NidSet.of(Get.identifierService().getConceptSequencesForConceptNids(possibleComponents));

            ConceptSequenceSet conceptSequences = Get.identifierService().getConceptSequencesForConceptNids(conceptsToIterateOver);
            Get.conceptService().getParallelConceptChronologyStream(conceptSequences).forEach((concept) -> {
                
                ConceptVersion mutable = concept.createMutableVersion(concept.getNid()); //TODO needs to return a mutable version, not a ConceptVersion

                ConceptChronology cch = (ConceptChronology)concept;
                Optional<LatestVersion<ConceptVersion<?>>> latest = cch.getLatestVersion(ConceptVersion.class, stampCoordinate);
                //Optional<LatestVersion<ConceptVersion<?>>> latest
                //        = ((ConceptChronology<ConceptVersion<?>>) concept).getLatestVersion(ConceptVersion.class, stampCoordinate);

                if (latest.isPresent()) {
                    rootClause[0].getChildren().stream().forEach((c) -> {
                        c.getQueryMatches(latest.get().value());
                    });
                }

            });
        }
        return rootClause[0].computeComponents(possibleComponents);
    }

    public PremiseType getPremiseType() {
        return premiseType;
    }

    public void setPremiseType(PremiseType premiseType) {
        this.premiseType = premiseType;
    }

    public LogicCoordinate getLogicCoordinate() {
        return logicCoordinate;
    }

    public void setLogicCoordinate(LogicCoordinate logicCoordinate) {
        this.logicCoordinate = logicCoordinate;
    }

    /**
     *
     * @return the <code>StampCoordinate</code> in the query
     */
    public StampCoordinate getStampCoordinate() {
        return stampCoordinate;
    }

    public void setStampCoordinate(StampCoordinate vc) {
        this.stampCoordinate = vc;
    }

    public LanguageCoordinate getLanguageCoordinate() {
        return languageCoordinate;
    }

    public void setLanguageCoordinate(LanguageCoordinate languageCoordinate) {
        this.languageCoordinate = languageCoordinate;
    }

    public static ArrayList<Object> returnDisplayObjects(NidSet resultSet, ReturnTypes returnType, StampCoordinate stampCoordinate,
            LanguageCoordinate languageCoordinate) throws ContradictionException, UnsupportedOperationException, IOException {
        TaxonomyCoordinateImpl taxonomyCoordinate = new TaxonomyCoordinateImpl(PremiseType.STATED, stampCoordinate, languageCoordinate, Get.configurationService().getDefaultLogicCoordinate());

        ArrayList<Object> results = new ArrayList<>();
        switch (returnType) {
            case UUIDS:
                resultSet.stream().forEach((nid) -> {
                    try {
                        ComponentReference cf = new ComponentReference(nid, stampCoordinate, languageCoordinate);
                        if (!cf.componentVersionIsNull()) {
                            results.add(cf.getUuid());
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                break;
            case NIDS:
                resultSet.stream().forEach((nid) -> {
                    results.add(nid);
                });
                break;
            case CONCEPT_VERSION:
                resultSet.stream().forEach((nid) -> {
                    try {
                        ConceptChronicleDdo cc = new ConceptChronicleDdo(taxonomyCoordinate, Ts.get().getConcept(nid),
                                RefexPolicy.REFEX_MEMBERS_AND_REFSET_MEMBERS, RelationshipPolicy.DESTINATION_RELATIONSHIPS);
                        results.add(cc);
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                });

                break;
            case COMPONENT:
                resultSet.stream().forEach((nid) -> {
                    try {
                        ComponentReference cf = new ComponentReference(nid, stampCoordinate, languageCoordinate);
                        if (!cf.componentVersionIsNull()) {
                            results.add(cf);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                break;
            case DESCRIPTION_FOR_COMPONENT:
            case DESCRIPTION_VERSION_FSN:
                resultSet.stream().forEach((nid) -> {
                    try {
                        ConceptChronology<? extends ConceptVersion> concept = Get.conceptService().getConcept(nid);
                        Optional<LatestVersion<DescriptionSememe<?>>> desc = concept.getFullySpecifiedDescription(languageCoordinate, stampCoordinate);
                        if (desc.isPresent()) {
                            LatestVersion<DescriptionSememe<?>> descVersion = desc.get();
                            ConceptChronicleDdo cc = new ConceptChronicleDdo(taxonomyCoordinate, concept,
                                    RefexPolicy.REFEX_MEMBERS_AND_REFSET_MEMBERS, RelationshipPolicy.DESTINATION_RELATIONSHIPS);
                            DescriptionChronicleDdo descChronicle = new DescriptionChronicleDdo(taxonomyCoordinate, cc, descVersion.value().getChronology());
                            DescriptionVersionDdo descVersionDdo = new DescriptionVersionDdo(descChronicle, taxonomyCoordinate, descVersion.value());
                            results.add(descVersionDdo);
                        }
                    } catch (IOException | ContradictionException ex) {
                        throw new RuntimeException(ex);
                    }
                });
                break;

            case DESCRIPTION_VERSION_PREFERRED:
                resultSet.stream().forEach((nid) -> {
                    try {
                        ConceptChronology<? extends ConceptVersion> concept = Get.conceptService().getConcept(nid);
                        Optional<LatestVersion<DescriptionSememe<?>>> desc = concept.getPreferredDescription(languageCoordinate, stampCoordinate);
                        if (desc.isPresent()) {
                            LatestVersion<DescriptionSememe<?>> descVersion = desc.get();
                            ConceptChronicleDdo cc = new ConceptChronicleDdo(taxonomyCoordinate, concept,
                                    RefexPolicy.REFEX_MEMBERS_AND_REFSET_MEMBERS, RelationshipPolicy.DESTINATION_RELATIONSHIPS);
                            DescriptionChronicleDdo descChronicle = new DescriptionChronicleDdo(taxonomyCoordinate, cc, descVersion.value().getChronology());
                            DescriptionVersionDdo descVersionDdo = new DescriptionVersionDdo(descChronicle, taxonomyCoordinate, descVersion.value());
                            results.add(descVersionDdo);
                        }
                    } catch (IOException | ContradictionException ex) {
                        throw new RuntimeException(ex);
                    }
                });
                break;
            default:
                throw new UnsupportedOperationException("Return type not supported.");
        }

        return results;
    }

    /**
     * The default method for computing query results, which returns the fully
     * specified description version of the components from the
     * <code>Query</code>.
     *
     * @return The result set of the <code>Query</code> in * * * * * * * * * *
     * an <code>ArrayList</code> of <code>DescriptionVersionDdo</code> objects
     * @throws IOException
     * @throws ContradictionException
     * @throws Exception
     */
    public ArrayList<Object> returnResults() throws IOException, ContradictionException, Exception {
        NidSet resultSet = compute();
        return returnDisplayObjects(resultSet, ReturnTypes.DESCRIPTION_VERSION_FSN);
    }

    /**
     * Return the desired Display Objects, which are specified by
     * <code>ReturnTypes</code>.
     *
     * @param resultSet results from the Query
     * @param returnType an <code>EnumSet</code> of <code>ReturnTypes</code>,
     * the desired Display Object types
     * @return an <code>ArrayList</code> of the Display Objects
     * @throws IOException
     * @throws ContradictionException
     */
    public ArrayList<Object> returnDisplayObjects(NidSet resultSet, ReturnTypes returnType) throws IOException, ContradictionException {
        return returnDisplayObjects(resultSet, returnType, stampCoordinate, languageCoordinate);

    }

    public void setResultSetLimit(int limit) {
        this.resultSetLimit = limit;
    }

    /**
     * Creates <code>ConceptIsKindOf</code> clause with default
     * <code>ViewCoordinate</code>.
     *
     * @param conceptSpecKey
     * @return
     */
    protected ConceptIsKindOf ConceptIsKindOf(String conceptSpecKey) {
        return new ConceptIsKindOf(this, conceptSpecKey, currentViewCoordinateKey);
    }

    /**
     * Creates <code>ConceptIsKindOf</code> clause with input
     * <code>ViewCoordinate</code>.
     *
     * @param conceptSpecKey
     * @param viewCoordinateKey
     * @return
     */
    protected ConceptIsKindOf ConceptIsKindOf(String conceptSpecKey, String viewCoordinateKey) {
        return new ConceptIsKindOf(this, conceptSpecKey, viewCoordinateKey);
    }

    protected DescriptionRegexMatch DescriptionRegexMatch(String regexKey) {
        return new DescriptionRegexMatch(this, regexKey, currentViewCoordinateKey);
    }

    protected DescriptionRegexMatch DescriptionRegexMatch(String regexKey, String viewCoordinateKey) {
        return new DescriptionRegexMatch(this, regexKey, viewCoordinateKey);
    }

    protected DescriptionActiveRegexMatch DescriptionActiveRegexMatch(String regexKey) {
        return new DescriptionActiveRegexMatch(this, regexKey, currentViewCoordinateKey);
    }

    protected DescriptionActiveRegexMatch DescriptionActiveRegexMatch(String regexKey, String viewCoordinateKey) {
        return new DescriptionActiveRegexMatch(this, regexKey, viewCoordinateKey);
    }

    /**
     * Creates <code>ConceptForComponent</code> clause with input child clause.
     *
     * @param child
     * @return
     */
    protected ConceptForComponent ConceptForComponent(Clause child) {
        return new ConceptForComponent(this, child);
    }

    protected ConceptIs ConceptIs(String conceptSpecKey) {
        return new ConceptIs(this, conceptSpecKey, currentViewCoordinateKey);
    }

    /**
     * Creates <code>ConceptIs</code> clause with input
     * <code>ViewCoordinate</code>.
     *
     * @param conceptSpecKey
     * @param viewCoordinateKey
     * @return
     */
    protected ConceptIs ConceptIs(String conceptSpecKey, String viewCoordinateKey) {
        return new ConceptIs(this, conceptSpecKey, viewCoordinateKey);
    }

    protected ConceptIsDescendentOf ConceptIsDescendentOf(String conceptSpecKey) {
        return new ConceptIsDescendentOf(this, conceptSpecKey, currentViewCoordinateKey);
    }

    /**
     * Creates <code>ConceptIsDescendentOf</code> clause with input
     * <code>ViewCoordinate</code>.
     *
     * @param conceptSpecKey
     * @param viewCoordinateKey
     * @return
     */
    protected ConceptIsDescendentOf ConceptIsDescendentOf(String conceptSpecKey, String viewCoordinateKey) {
        return new ConceptIsDescendentOf(this, conceptSpecKey, viewCoordinateKey);
    }

    protected ConceptIsChildOf ConceptIsChildOf(String conceptSpecKey) {
        return new ConceptIsChildOf(this, conceptSpecKey, currentViewCoordinateKey);
    }

    /**
     * Creates <code>ConceptIsChildOf</code> clause with input
     * <code>ViewCoordinate</code>.
     *
     * @param conceptSpecKey
     * @param viewCoordinateKey
     * @return
     */
    protected ConceptIsChildOf ConceptIsChildOf(String conceptSpecKey, String viewCoordinateKey) {
        return new ConceptIsChildOf(this, conceptSpecKey, viewCoordinateKey);
    }

    protected DescriptionActiveLuceneMatch DescriptionActiveLuceneMatch(String queryTextKey) {
        return new DescriptionActiveLuceneMatch(this, queryTextKey, currentViewCoordinateKey);
    }

    protected DescriptionActiveLuceneMatch DescriptionActiveLuceneMatch(String queryTextKey, String viewCoordinateKey) {
        return new DescriptionActiveLuceneMatch(this, queryTextKey, viewCoordinateKey);
    }

    protected DescriptionLuceneMatch DescriptionLuceneMatch(String queryTextKey) {
        return new DescriptionLuceneMatch(this, queryTextKey, currentViewCoordinateKey);
    }

    protected And And(Clause... clauses) {
        return new And(this, clauses);
    }

    protected RelRestriction RelRestriction(String relTypeKey, String destinationSpecKey) {
        return new RelRestriction(this, relTypeKey, destinationSpecKey, currentViewCoordinateKey, null, null);
    }

    protected RelRestriction RelRestriction(String relTypeKey, String destinationSpecKey, String key) {
        if (this.letDeclarations.get(key) instanceof Boolean) {
            return new RelRestriction(this, relTypeKey, destinationSpecKey, currentViewCoordinateKey, key, null);
        } else {
            return new RelRestriction(this, relTypeKey, destinationSpecKey, key, null, null);
        }
    }

    protected RelRestriction RelRestriction(String relTypeKey, String destinatonSpecKey, String relTypeSubsumptionKey, String targetSubsumptionKey) {
        return new RelRestriction(this, relTypeKey, destinatonSpecKey, currentViewCoordinateKey, relTypeSubsumptionKey, targetSubsumptionKey);

    }

    protected RelRestriction RelRestriction(String relTypeKey, String destinationSpecKey, String viewCoordinateKey, String relTypeSubsumptionKey, String targetSubsumptionKey) {
        return new RelRestriction(this, relTypeKey, destinationSpecKey, viewCoordinateKey, relTypeSubsumptionKey, targetSubsumptionKey);
    }

    protected RefsetContainsConcept RefsetContainsConcept(String refsetSpecKey, String conceptSpecKey) {
        return new RefsetContainsConcept(this, refsetSpecKey, conceptSpecKey, currentViewCoordinateKey);
    }

    protected RefsetContainsConcept RefsetContainsConcept(String refsetSpecKey, String conceptSpecKey, String viewCoordinateKey) {
        return new RefsetContainsConcept(this, refsetSpecKey, conceptSpecKey, viewCoordinateKey);
    }

    protected RefsetContainsKindOfConcept RefsetContainsKindOfConcept(String refsetSpecKey, String conceptSpecKey) {
        return new RefsetContainsKindOfConcept(this, refsetSpecKey, conceptSpecKey, currentViewCoordinateKey);
    }

    protected RefsetContainsKindOfConcept RefsetContainsKindOfConcept(String refsetSpecKey, String conceptSpecKey, String viewCoordinateKey) {
        return new RefsetContainsKindOfConcept(this, refsetSpecKey, conceptSpecKey, viewCoordinateKey);
    }

    protected RefsetContainsString RefsetContainsString(String refsetSpecKey, String stringMatchKey) {
        return new RefsetContainsString(this, refsetSpecKey, stringMatchKey, currentViewCoordinateKey);
    }

    protected RefsetContainsString RefsetContainsString(String refsetSpecKey, String stringMatchKey, String viewCoordinateKey) {
        return new RefsetContainsString(this, refsetSpecKey, stringMatchKey, viewCoordinateKey);
    }

    protected RefsetLuceneMatch RefsetLuceneMatch(String queryString) {
        return new RefsetLuceneMatch(this, queryString, currentViewCoordinateKey);
    }

    protected PreferredNameForConcept PreferredNameForConcept(Clause clause) {
        return new PreferredNameForConcept(this, clause);
    }

    protected And Intersection(Clause... clauses) {
        return new And(this, clauses);
    }

    protected FullySpecifiedNameForConcept FullySpecifiedNameForConcept(Clause clause) {
        return new FullySpecifiedNameForConcept(this, clause);
    }

    public Not Not(Clause clause) {
        return new Not(this, clause);
    }

    /**
     * Getter for the For set.
     *
     * @return the <code>NativeIdSetBI</code> of the concepts that will be
     * searched in the query
     */
    public NidSet getForSet() {
        return forSet;
    }

    protected Or Or(Clause... clauses) {
        return new Or(this, clauses);
    }

    protected Or Union(Clause... clauses) {
        return new Or(this, clauses);
    }

    protected ChangedFromPreviousVersion ChangedFromPreviousVersion(String previousCoordinateKey) {
        return new ChangedFromPreviousVersion(this, previousCoordinateKey);
    }

    protected Xor Xor(Clause... clauses) {
        return new Xor(this, clauses);
    }

    protected AndNot AndNot(Clause... clauses) {
        return new AndNot(this, clauses);
    }
}
