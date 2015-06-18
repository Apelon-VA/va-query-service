/*
 * Copyright 2015 U.S. Department of Veterans Affairs.
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
package gov.vha.isaac.cradle.tasks;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.cradle.taxonomy.DestinationOriginRecord;
import gov.vha.isaac.cradle.taxonomy.TaxonomyFlags;
import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordPrimitive;
import gov.vha.isaac.cradle.waitfree.CasSequenceObjectMap;
import gov.vha.isaac.metadata.coordinates.LogicCoordinates;
import gov.vha.isaac.metadata.coordinates.StampCoordinates;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.DataSource;
import gov.vha.isaac.ochre.api.DataTarget;
import gov.vha.isaac.ochre.api.IdentifierService;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.State;
import gov.vha.isaac.ochre.api.chronicle.LatestVersion;
import gov.vha.isaac.ochre.api.commit.CommitService;
import gov.vha.isaac.ochre.api.component.concept.ConceptService;
import gov.vha.isaac.ochre.api.component.concept.ConceptServiceManagerI;
import gov.vha.isaac.ochre.api.component.sememe.SememeBuilder;
import gov.vha.isaac.ochre.api.component.sememe.SememeBuilderService;
import gov.vha.isaac.ochre.api.component.sememe.SememeChronology;
import gov.vha.isaac.ochre.api.component.sememe.SememeService;
import gov.vha.isaac.ochre.api.component.sememe.version.LogicGraphSememe;
import gov.vha.isaac.ochre.api.component.sememe.version.MutableLogicGraphSememe;
import gov.vha.isaac.ochre.api.coordinate.LogicCoordinate;
import gov.vha.isaac.ochre.api.coordinate.StampCoordinate;
import gov.vha.isaac.ochre.api.coordinate.StampPrecedence;
import gov.vha.isaac.ochre.api.logic.LogicalExpression;
import gov.vha.isaac.ochre.api.logic.LogicalExpressionBuilder;
import gov.vha.isaac.ochre.api.logic.LogicalExpressionBuilderService;
import static gov.vha.isaac.ochre.api.logic.LogicalExpressionBuilder.*;
import gov.vha.isaac.ochre.api.logic.assertions.Assertion;
import gov.vha.isaac.ochre.api.snapshot.calculator.RelativePosition;
import gov.vha.isaac.ochre.api.snapshot.calculator.RelativePositionCalculator;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import gov.vha.isaac.ochre.model.concept.ConceptChronologyImpl;
import gov.vha.isaac.ochre.model.coordinate.StampCoordinateImpl;
import gov.vha.isaac.ochre.model.coordinate.StampPositionImpl;
import gov.vha.isaac.ochre.model.logic.LogicalExpressionOchreImpl;
import gov.vha.isaac.ochre.api.logic.Node;
import gov.vha.isaac.ochre.model.logic.node.AndNode;
import gov.vha.isaac.ochre.model.logic.node.internal.ConceptNodeWithNids;
import gov.vha.isaac.ochre.model.logic.node.internal.RoleNodeSomeWithNids;
import gov.vha.isaac.ochre.model.sememe.SememeChronologyImpl;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.mahout.math.map.OpenIntObjectHashMap;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;
import org.ihtsdo.otf.tcc.dto.TtkConceptChronicle;
import org.ihtsdo.otf.tcc.dto.component.attribute.TtkConceptAttributesVersion;
import org.ihtsdo.otf.tcc.dto.component.relationship.TtkRelationshipVersion;

/**
 *
 * @author kec
 */
public class ConvertOtfToOchreModel implements Callable<Void> {

    private static final Logger log = LogManager.getLogger();

    private static final CommitService commitService = LookupService.getService(CommitService.class);
    private static final SememeService sememeService = LookupService.getService(SememeService.class);
    private static final IdentifierService identifierService = LookupService.getService(IdentifierService.class);
    private static final ConceptService conceptService = LookupService.getService(ConceptServiceManagerI.class).get();
    private static final CradleExtensions cradle = LookupService.getService(CradleExtensions.class);
    private static final SememeBuilderService sememeBuilderService = LookupService.getService(SememeBuilderService.class);
    private static final LogicalExpressionBuilderService expressionBuilderService
            = LookupService.getService(LogicalExpressionBuilderService.class);

    private static final int isaSequence;

    public static CasSequenceObjectMap<TaxonomyRecordPrimitive> originDestinationTaxonomyRecords;
    public static ConcurrentSkipListSet<DestinationOriginRecord> destinationOriginRecordSet;
    private static final ConceptSequenceSet neverRoleGroupConceptSequences = new ConceptSequenceSet();

    static {
        originDestinationTaxonomyRecords = cradle.getOriginDestinationTaxonomyMap();
        destinationOriginRecordSet = cradle.getDestinationOriginRecordSet();
        isaSequence = identifierService.getConceptSequenceForUuids(IsaacMetadataAuxiliaryBinding.IS_A.getUuids());
        neverRoleGroupConceptSequences.add(identifierService.getConceptSequence(Snomed.PART_OF.getNid()));
        neverRoleGroupConceptSequences.add(identifierService.getConceptSequence(Snomed.LATERALITY.getNid()));
        neverRoleGroupConceptSequences.add(identifierService.getConceptSequence(Snomed.HAS_ACTIVE_INGREDIENT.getNid()));
        neverRoleGroupConceptSequences.add(identifierService.getConceptSequence(Snomed.HAS_DOSE_FORM.getNid()));
    }

    TtkConceptChronicle eConcept;
    UUID newPathUuid = null;
    int lastRelCharacteristic = Integer.MAX_VALUE;
    SememeChronology<LogicGraphSememe> statedChronology = null;
    SememeChronology<LogicGraphSememe> inferredChronology = null;
    StampCoordinate latestOnDevCoordinate = StampCoordinates.getDevelopmentLatest();
    LogicCoordinate logicCoordinate = LogicCoordinates.getStandardElProfile();

    public ConvertOtfToOchreModel(TtkConceptChronicle eConcept, UUID newPathUuid) {
        this(eConcept);
        this.newPathUuid = newPathUuid;
    }

    public ConvertOtfToOchreModel(TtkConceptChronicle eConcept) {
        this.eConcept = eConcept;
    }

    @Override
    public Void call() throws Exception {
        
//        if (eConcept.getPrimordialUuid().equals(UUID.fromString("164d0c37-67b3-3bd3-b304-79d09c3f1411"))) {
//            log.info("Found watch");
//        }

        try {
            if (this.newPathUuid != null) {
                eConcept.processComponentRevisions(r -> r.setPathUuid(newPathUuid));
            }

            ConceptChronologyImpl conceptChronology
                    = (ConceptChronologyImpl) conceptService.getConcept(eConcept.getUuidList().toArray(new UUID[0]));

            TreeSet<StampPositionImpl> stampPositionSet = new TreeSet<>();
            eConcept.getStampSequenceStream().distinct().forEach((stampSequence) -> {
                stampPositionSet.add(new StampPositionImpl(
                        commitService.getTimeForStamp(stampSequence),
                        commitService.getPathSequenceForStamp(stampSequence)));
            });
            // Create a logical definition corresponding with each unique
            // stamp position in the concept
            stampPositionSet.forEach((stampPosition) -> {
                StampCoordinateImpl stampCoordinate
                        = new StampCoordinateImpl(StampPrecedence.PATH, stampPosition, null);
                RelativePositionCalculator calc = RelativePositionCalculator.getCalculator(stampCoordinate);
                Optional<LatestVersion<TtkConceptAttributesVersion>> latestAttributeVersion
                        = calc.getLatestVersion(eConcept.getConceptAttributes());
                if (latestAttributeVersion.isPresent()) {
                    int moduleSequence = latestAttributeVersion.get().value().getModuleSequence();
                    LogicalExpressionBuilder inferredBuilder = expressionBuilderService.getLogicalExpressionBuilder();
                    LogicalExpressionBuilder statedBuilder = expressionBuilderService.getLogicalExpressionBuilder();
                    if (latestAttributeVersion.get().value().isDefined()) {
                        Assertion[] inferredAssertions = makeAssertions(calc, inferredBuilder,
                                IsaacMetadataAuxiliaryBinding.INFERRED.getPrimodialUuid());
                        Assertion[] statedAssertions = makeAssertions(calc, statedBuilder,
                                IsaacMetadataAuxiliaryBinding.STATED.getPrimodialUuid());
                        if (statedAssertions.length > 0) {
                            SufficientSet(And(statedAssertions));
                        }
                        if (inferredAssertions.length > 0) {
                            SufficientSet(And(inferredAssertions));
                        }

                    } else {
                        Assertion[] inferredAssertions = makeAssertions(calc, inferredBuilder,
                                IsaacMetadataAuxiliaryBinding.INFERRED.getPrimodialUuid());
                        Assertion[] statedAssertions = makeAssertions(calc, statedBuilder,
                                IsaacMetadataAuxiliaryBinding.STATED.getPrimodialUuid());
                        if (inferredAssertions.length > 0) {
                            NecessarySet(And(inferredAssertions));
                        }
                        if (statedAssertions.length > 0) {
                            NecessarySet(And(statedAssertions));
                        }
                    }
                    LogicalExpression inferredExpression = inferredBuilder.build();
                    if (inferredExpression.isMeaningful()) {
                        int stampSequence = commitService.getStampSequence(State.ACTIVE, stampPosition.getTime(),
                                IsaacMetadataAuxiliaryBinding.IHTSDO_CLASSIFIER.getSequence(),
                                moduleSequence, stampPosition.getStampPathSequence());
                        if (inferredChronology == null) {
                            SememeBuilder<SememeChronology<LogicGraphSememe>> builder
                                    = sememeBuilderService.getLogicalExpressionSememeBuilder(inferredExpression,
                                            conceptChronology.getNid(), logicCoordinate.getInferredAssemblageSequence());
                            inferredChronology = builder.build(stampSequence, new ArrayList());
                        } else {
                            MutableLogicGraphSememe newVersion = inferredChronology.createMutableVersion(MutableLogicGraphSememe.class, stampSequence);
                            newVersion.setGraphData(inferredExpression.getData(DataTarget.INTERNAL));
                        }
                    }
                    LogicalExpression statedExpression = statedBuilder.build();
                    if (!statedExpression.isMeaningful()) {
                        // substitute inferred expression, as early SNOMED stated expressions where lost. 
                        statedExpression = inferredExpression;
                    }
                    if (statedExpression.isMeaningful()) {
                        int stampSequence = commitService.getStampSequence(State.ACTIVE, stampPosition.getTime(),
                                IsaacMetadataAuxiliaryBinding.USER.getSequence(),
                                moduleSequence, stampPosition.getStampPathSequence());
                        if (statedChronology == null) {
                            SememeBuilder<SememeChronology<LogicGraphSememe>> builder
                                    = sememeBuilderService.getLogicalExpressionSememeBuilder(statedExpression,
                                            conceptChronology.getNid(), logicCoordinate.getStatedAssemblageSequence());
                            statedChronology = builder.build(stampSequence, new ArrayList());
                        } else {
                            MutableLogicGraphSememe newVersion = statedChronology.createMutableVersion(MutableLogicGraphSememe.class, stampSequence);
                            newVersion.setGraphData(statedExpression.getData(DataTarget.INTERNAL));
                        }
                    }

                }
            });

            if (statedChronology != null) {
                removeDuplicates(statedChronology);
                extractTaxonomy(conceptChronology, statedChronology, TaxonomyFlags.STATED);
                sememeService.writeSememe(statedChronology);
            }
            if (inferredChronology != null) {
                removeDuplicates(inferredChronology);
                extractTaxonomy(conceptChronology, inferredChronology, TaxonomyFlags.INFERRED);
                sememeService.writeSememe(inferredChronology);
            }

            return null;
        } catch (Exception e) {
            log.error("Failure converting " + eConcept.toString(), e);
            throw e;
        }
    }

    private Assertion[] makeAssertions(RelativePositionCalculator calc,
            LogicalExpressionBuilder defBuilder, UUID characteristicUuid) {
        List<Assertion> assertionList = new ArrayList<>();
        OpenIntObjectHashMap<List<Assertion>> relGroupMap = new OpenIntObjectHashMap<>();
        eConcept.getRelationships().forEach((relationship) -> {
            Optional<LatestVersion<TtkRelationshipVersion>> latestRelVersion
                    = calc.getLatestVersion(relationship);
            if (latestRelVersion.isPresent()) {
                TtkRelationshipVersion relVersion = latestRelVersion.get().value();
                if (relVersion.getState() == State.ACTIVE
                        && relVersion.getCharacteristicUuid().equals(characteristicUuid)) {
                    if (relVersion.getGroup() == 0) {
                        int typeSequence = identifierService.getConceptSequenceForUuids(relVersion.getTypeUuid());
                        if (typeSequence == isaSequence) {
                            assertionList.add(ConceptAssertion(conceptService.getConcept(relationship.c2Uuid), defBuilder));
                        } else {
                            if (neverRoleGroupConceptSequences.contains(typeSequence)) {
                                assertionList.add(SomeRole(conceptService.getConcept(relVersion.getTypeUuid()),
                                        ConceptAssertion(conceptService.getConcept(relationship.c2Uuid), defBuilder)));
                            } else {
                                assertionList.add(
                                        SomeRole(conceptService.getConcept(IsaacMetadataAuxiliaryBinding.ROLE_GROUP.getUuids()),
                                                And(
                                                    SomeRole(conceptService.getConcept(relVersion.getTypeUuid()),
                                                        ConceptAssertion(conceptService.getConcept(relationship.c2Uuid), defBuilder)))));
                            }
                        }

                    } else {
                        if (!relGroupMap.containsKey(relVersion.getGroup())) {
                            relGroupMap.put(relVersion.getGroup(), new ArrayList<>());
                        }
                        relGroupMap.get(relVersion.getGroup()).add(
                                SomeRole(conceptService.getConcept(relVersion.getTypeUuid()),
                                        ConceptAssertion(conceptService.getConcept(relationship.c2Uuid), defBuilder))
                        );
                    }
                }
            }
        });
        // process rel groups here...
        relGroupMap.forEachPair((group, assertionsInRelGroupList) -> {
            assertionList.add(
                    SomeRole(
                        conceptService.getConcept(IsaacMetadataAuxiliaryBinding.ROLE_GROUP.getUuids()),
                        And(assertionsInRelGroupList.toArray(new Assertion[assertionsInRelGroupList.size()]))));
            return true;
        });
        return assertionList.toArray(new Assertion[assertionList.size()]);
    }

    private void extractTaxonomy(ConceptChronologyImpl conceptChronology,
            SememeChronology<LogicGraphSememe> logicGraphChronology,
            TaxonomyFlags taxonomyFlags) {
        Optional<TaxonomyRecordPrimitive> record = originDestinationTaxonomyRecords.get(conceptChronology.getConceptSequence());

        TaxonomyRecordPrimitive parentTaxonomyRecord;
        if (record.isPresent()) {
            parentTaxonomyRecord = record.get();
        } else {
            parentTaxonomyRecord = new TaxonomyRecordPrimitive();
        }

        logicGraphChronology.getVersionList().forEach((logicVersion) -> {
             LogicalExpressionOchreImpl expression
                    = new LogicalExpressionOchreImpl(logicVersion.getGraphData(),
                            DataSource.INTERNAL,
                            conceptChronology.getConceptSequence());

            expression.getRoot()
                    .getChildStream().forEach((necessaryOrSufficientSet) -> {
                        necessaryOrSufficientSet.getChildStream().forEach((Node andOrOrNode)
                                -> andOrOrNode.getChildStream().forEach((Node aNode) -> {
                            switch (aNode.getNodeSemantic()) {
                                case CONCEPT:
                                    createIsaRel((ConceptNodeWithNids) aNode, parentTaxonomyRecord, 
                                            taxonomyFlags, logicVersion.getStampSequence());
                                    break;
                                case ROLE_SOME:
                                    createSomeRole((RoleNodeSomeWithNids) aNode, parentTaxonomyRecord, 
                                            taxonomyFlags, logicVersion.getStampSequence());
                                    break;
                                default:
                                    throw new UnsupportedOperationException("Can't handle: " + aNode.getNodeSemantic());
                            }
                        }));
                    });
        });
        originDestinationTaxonomyRecords.put(conceptChronology.getConceptSequence(), parentTaxonomyRecord);
    }

    private void createIsaRel(ConceptNodeWithNids conceptNode, 
            TaxonomyRecordPrimitive parentTaxonomyRecord, 
            TaxonomyFlags taxonomyFlags, int stampSequence) {
        parentTaxonomyRecord.getTaxonomyRecordUnpacked()
                .addStampRecord(conceptNode.getConceptNid(), isaSequence,
                        stampSequence, taxonomyFlags.bits);
    }

    private void createSomeRole(RoleNodeSomeWithNids someNode, 
            TaxonomyRecordPrimitive parentTaxonomyRecord, 
            TaxonomyFlags taxonomyFlags, int stampSequence) {
        
            if (someNode.getTypeConceptNid() == IsaacMetadataAuxiliaryBinding.ROLE_GROUP.getNid()) {
                AndNode andNode = (AndNode) someNode.getOnlyChild();
                andNode.getChildStream().forEach((roleGroupSomeNode) -> {
                    createSomeRole((RoleNodeSomeWithNids) roleGroupSomeNode, 
                            parentTaxonomyRecord, taxonomyFlags, stampSequence);
                });
                
            } else {
                ConceptNodeWithNids restrictionNode = (ConceptNodeWithNids) someNode.getOnlyChild();
                parentTaxonomyRecord.getTaxonomyRecordUnpacked()
                            .addStampRecord(restrictionNode.getConceptNid(), restrictionNode.getConceptNid(), 
                                    stampSequence, taxonomyFlags.bits);
            }
    }

    private void removeDuplicates(SememeChronology<LogicGraphSememe> logicChronology) {
        
        RelativePositionCalculator calc = RelativePositionCalculator.getCalculator(latestOnDevCoordinate);
        SortedSet<LogicGraphSememe> sortedLogicGraphs = new TreeSet<>((LogicGraphSememe graph1, LogicGraphSememe graph2) -> {
            RelativePosition relativePosition = calc.fastRelativePosition(graph1, graph2, latestOnDevCoordinate.getStampPrecedence());
            switch (relativePosition) {
                case BEFORE:
                    return -1;
                case EQUAL:
                    return 0;
                case AFTER:
                    return 1;
                case UNREACHABLE:
                case CONTRADICTION:
                default:
                    throw new UnsupportedOperationException("Can't handle: " + relativePosition);
            }
        });
        
        sortedLogicGraphs.addAll(logicChronology.getVersionList());
        
        List<LogicGraphSememe> uniqueGraphs = new ArrayList();
        LogicGraphSememe lastGraph = null;
        
        for (LogicGraphSememe graphToTest: sortedLogicGraphs) {
            if (lastGraph == null) {
                lastGraph = graphToTest;
                uniqueGraphs.add(graphToTest);
            } else {
                LogicalExpression lastExpression = expressionBuilderService.fromSememe(lastGraph);
                LogicalExpression expressionToTest = expressionBuilderService.fromSememe(graphToTest);
                if (commitService.stampSequencesEqualExceptAuthorAndTime(
                        lastGraph.getStampSequence(), graphToTest.getStampSequence())
                     &&
                        !lastExpression.equals(expressionToTest)) {
                    lastGraph = graphToTest;
                    uniqueGraphs.add(graphToTest);
                }
            }
        }
        ((SememeChronologyImpl)logicChronology).setVersions(uniqueGraphs);
    }
}
