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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY_STATE_SET KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gov.vha.isaac.cradle.tasks;

import gov.vha.isaac.metadata.coordinates.LogicCoordinates;
import gov.vha.isaac.metadata.coordinates.StampCoordinates;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.DataTarget;
import gov.vha.isaac.ochre.api.Get;
import gov.vha.isaac.ochre.api.State;
import gov.vha.isaac.ochre.api.chronicle.LatestVersion;
import gov.vha.isaac.ochre.api.component.concept.ConceptChronology;
import gov.vha.isaac.ochre.api.component.sememe.SememeBuilder;
import gov.vha.isaac.ochre.api.component.sememe.SememeChronology;
import gov.vha.isaac.ochre.api.component.sememe.SememeConstraints;
import gov.vha.isaac.ochre.api.component.sememe.version.LogicGraphSememe;
import gov.vha.isaac.ochre.api.component.sememe.version.MutableLogicGraphSememe;
import gov.vha.isaac.ochre.api.coordinate.LogicCoordinate;
import gov.vha.isaac.ochre.api.coordinate.PremiseType;
import gov.vha.isaac.ochre.api.coordinate.StampCoordinate;
import gov.vha.isaac.ochre.api.coordinate.StampPrecedence;
import gov.vha.isaac.ochre.api.logic.LogicalExpression;
import gov.vha.isaac.ochre.api.logic.LogicalExpressionBuilder;
import static gov.vha.isaac.ochre.api.logic.LogicalExpressionBuilder.*;
import gov.vha.isaac.ochre.api.logic.assertions.Assertion;
import gov.vha.isaac.ochre.api.snapshot.calculator.RelativePosition;
import gov.vha.isaac.ochre.api.snapshot.calculator.RelativePositionCalculator;
import gov.vha.isaac.ochre.collections.SememeSequenceSet;
import gov.vha.isaac.ochre.model.coordinate.StampCoordinateImpl;
import gov.vha.isaac.ochre.model.coordinate.StampPositionImpl;
import gov.vha.isaac.ochre.model.sememe.SememeChronologyImpl;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.mahout.math.map.OpenIntObjectHashMap;
import org.ihtsdo.otf.tcc.dto.TtkConceptChronicle;
import org.ihtsdo.otf.tcc.dto.TtkConceptLock;
import org.ihtsdo.otf.tcc.dto.component.attribute.TtkConceptAttributesVersion;
import org.ihtsdo.otf.tcc.dto.component.relationship.TtkRelationshipVersion;

/**
 *
 * @author kec
 */
public class ConvertOtfToOchreModel implements Callable<Void> {

    private static final Logger log = LogManager.getLogger();

    private static final boolean VERBOSE = false;

    private TtkConceptChronicle eConcept;
    private UUID newPathUuid = null;
    private SememeChronology<LogicGraphSememe<?>> statedChronology = null;
    private SememeChronology<LogicGraphSememe<?>> inferredChronology = null;
    private final StampCoordinate latestOnDevCoordinate = StampCoordinates.getDevelopmentLatest();
    private final LogicCoordinate logicCoordinate = LogicCoordinates.getStandardElProfile();
    private final ImportEConceptFile parentTask;

    public ConvertOtfToOchreModel(TtkConceptChronicle eConcept, UUID newPathUuid, ImportEConceptFile parentTask) {
        this(eConcept, parentTask);
        this.newPathUuid = newPathUuid;
    }

    public ConvertOtfToOchreModel(TtkConceptChronicle eConcept, ImportEConceptFile parentTask) {
        this.eConcept = eConcept;
        this.parentTask = parentTask;
    }

    @Override
    public Void call() throws Exception {

        TtkConceptLock.getLock(eConcept.getUuidList()).lock();
        try {
            if (this.newPathUuid != null) {
                eConcept.processComponentRevisions(r -> r.setPathUuid(newPathUuid));
            }

            ConceptChronology conceptChronology
                    = Get.conceptService().getConcept(eConcept.getUuidList().toArray(new UUID[0]));
            SememeSequenceSet inferredSememeSequences
                    = Get.sememeService().getSememeSequencesForComponentFromAssemblage(Get.identifierService().getConceptNid(conceptChronology.getConceptSequence()), logicCoordinate.getInferredAssemblageSequence());
            SememeSequenceSet statedSememeSequences
                    = Get.sememeService().getSememeSequencesForComponentFromAssemblage(Get.identifierService().getConceptNid(conceptChronology.getConceptSequence()), logicCoordinate.getStatedAssemblageSequence());

            if (!inferredSememeSequences.isEmpty()) {
                if (inferredSememeSequences.size() > 1) {
                    throw new IllegalStateException("Error importing: " + conceptChronology.toUserString()
                            + "<" + conceptChronology.getConceptSequence() + "> Found more than one inferred definition"
                            + inferredSememeSequences + "\n eConcept: " + eConcept);
                }
                inferredChronology = (SememeChronology<LogicGraphSememe<?>>) Get.sememeService().getSememe(inferredSememeSequences.findFirst().getAsInt());
            }
            if (!statedSememeSequences.isEmpty()) {
                if (statedSememeSequences.size() > 1) {
                    throw new IllegalStateException("Error importing: " + conceptChronology.toUserString()
                            + "<" + conceptChronology.getConceptSequence() + "> Found more than one stated definition"
                            + statedSememeSequences + "\n eConcept: " + eConcept);
                }
                statedChronology = (SememeChronology<LogicGraphSememe<?>>) Get.sememeService().getSememe(statedSememeSequences.findFirst().getAsInt());
            }
            TreeSet<StampPositionImpl> stampPositionSet = new TreeSet<>();
            eConcept.getStampSequenceStream().distinct().forEach((stampSequence) -> {
                stampPositionSet.add(new StampPositionImpl(
                        Get.commitService().getTimeForStamp(stampSequence),
                        Get.commitService().getPathSequenceForStamp(stampSequence)));
            });
            // Create a logical definition corresponding with each unique
            // stamp position in the concept
            stampPositionSet.forEach((stampPosition) -> {
                StampCoordinateImpl stampCoordinate
                        = new StampCoordinateImpl(StampPrecedence.PATH, stampPosition, null, State.ANY_STATE_SET);
                RelativePositionCalculator calc = RelativePositionCalculator.getCalculator(stampCoordinate);
                Optional<LatestVersion<TtkConceptAttributesVersion>> latestAttributeVersion
                        = calc.getLatestVersion(eConcept.getConceptAttributes());
                if (latestAttributeVersion.isPresent()) {
                    int moduleSequence = latestAttributeVersion.get().value().getModuleSequence();
                    LogicalExpressionBuilder inferredBuilder = parentTask.expressionBuilderService.getLogicalExpressionBuilder();
                    LogicalExpressionBuilder statedBuilder = parentTask.expressionBuilderService.getLogicalExpressionBuilder();
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
                        printIfMoreNodes(conceptChronology, inferredExpression);
                        int stampSequence = Get.commitService().getStampSequence(State.ACTIVE, stampPosition.getTime(),
                                IsaacMetadataAuxiliaryBinding.IHTSDO_CLASSIFIER.getSequence(),
                                moduleSequence, stampPosition.getStampPathSequence());
                        if (inferredChronology == null) {
                            SememeBuilder<SememeChronology<LogicGraphSememe<?>>> builder
                                    = parentTask.sememeBuilderService.getLogicalExpressionSememeBuilder(inferredExpression,
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
                        printIfMoreNodes(conceptChronology, statedExpression);
                        int stampSequence = Get.commitService().getStampSequence(State.ACTIVE, stampPosition.getTime(),
                                IsaacMetadataAuxiliaryBinding.USER.getSequence(),
                                moduleSequence, stampPosition.getStampPathSequence());
                        if (statedChronology == null) {
                            SememeBuilder<SememeChronology<LogicGraphSememe<?>>> builder
                                    = parentTask.sememeBuilderService.getLogicalExpressionSememeBuilder(statedExpression,
                                            conceptChronology.getNid(), logicCoordinate.getStatedAssemblageSequence());
                            statedChronology = builder.build(stampSequence, new ArrayList());
                        } else {
                            MutableLogicGraphSememe newVersion = statedChronology.createMutableVersion(MutableLogicGraphSememe.class, stampSequence);
                            newVersion.setGraphData(statedExpression.getData(DataTarget.INTERNAL));
                        }
                    }

                }
            });
            if (eConcept.getPrimordialUuid().equals(UUID.fromString("128f5d97-8523-38d5-addc-a691fd8a3674"))
                    || eConcept.getPrimordialUuid().equals(UUID.fromString("128f5d97-8523-38d5-addc-a691fd8a3674"))) {
                log.info("Found watch");
            }

            if (statedChronology != null) {
                removeDuplicates(statedChronology);
                Get.taxonomyService().updateTaxonomy(statedChronology);
                Get.sememeService().writeSememe(statedChronology, SememeConstraints.ONE_SEMEME_PER_COMPONENT);
                if (VERBOSE && statedChronology.getVersionStampSequences().count() > parentTask.maxDefinitionVersionCount.get()) {
                    parentTask.maxDefinitionVersionCount.set((int) statedChronology.getVersionStampSequences().count());
                    String report = conceptChronology.getLogicalDefinitionChronologyReport(latestOnDevCoordinate, PremiseType.STATED, logicCoordinate);
                    log.info("\n" + report);
                }
            }
            if (inferredChronology != null) {
                removeDuplicates(inferredChronology);
                Get.taxonomyService().updateTaxonomy(inferredChronology);
                Get.sememeService().writeSememe(inferredChronology, SememeConstraints.ONE_SEMEME_PER_COMPONENT);
                if (VERBOSE && inferredChronology.getVersionStampSequences().count() > parentTask.maxDefinitionVersionCount.get()) {
                    parentTask.maxDefinitionVersionCount.set((int) inferredChronology.getVersionStampSequences().count());
                    String report = conceptChronology.getLogicalDefinitionChronologyReport(latestOnDevCoordinate, PremiseType.INFERRED, logicCoordinate);
                    log.info("\n" + report);
                }
            }

            return null;
        } catch (Exception e) {
            log.error("Failure converting " + eConcept.toString(), e);
            throw e;
        } finally {
            TtkConceptLock.getLock(eConcept.getUuidList()).unlock();
        }
    }

    private void printIfMoreNodes(ConceptChronology conceptChronicle, LogicalExpression logicGraph) {
        if (VERBOSE) {
            if (logicGraph.getNodeCount() > parentTask.maxDefinitionNodeCount.get()) {
                parentTask.maxDefinitionNodeCount.set(logicGraph.getNodeCount());
                StringBuilder builder = new StringBuilder();
                builder.append("================================================================================\n");
                builder.append(" Encountered concept '")
                        .append(Get.conceptDescriptionText(conceptChronicle.getNid()))
                        .append("' with ").append(logicGraph.getNodeCount())
                        .append(" nodes in definition:\n");
                builder.append("================================================================================\n");
                builder.append(logicGraph);
                builder.append("================================================================================\n");
                System.out.println(builder.toString());
            }
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
                        int typeSequence = Get.identifierService().getConceptSequenceForUuids(relVersion.getTypeUuid());
                        if (typeSequence == parentTask.isaSequence) {
                            assertionList.add(ConceptAssertion(Get.conceptService().getConcept(relationship.c2Uuid), defBuilder));
                        } else {
                            if (parentTask.neverRoleGroupConceptSequences.contains(typeSequence)) {
                                assertionList.add(SomeRole(Get.conceptService().getConcept(relVersion.getTypeUuid()),
                                        ConceptAssertion(Get.conceptService().getConcept(relationship.c2Uuid), defBuilder)));
                            } else {
                                assertionList.add(
                                        SomeRole(Get.conceptService().getConcept(IsaacMetadataAuxiliaryBinding.ROLE_GROUP.getUuids()),
                                                And(
                                                        SomeRole(Get.conceptService().getConcept(relVersion.getTypeUuid()),
                                                                ConceptAssertion(Get.conceptService().getConcept(relationship.c2Uuid), defBuilder)))));
                            }
                        }

                    } else {
                        if (!relGroupMap.containsKey(relVersion.getGroup())) {
                            relGroupMap.put(relVersion.getGroup(), new ArrayList<>());
                        }
                        relGroupMap.get(relVersion.getGroup()).add(
                                SomeRole(Get.conceptService().getConcept(relVersion.getTypeUuid()),
                                        ConceptAssertion(Get.conceptService().getConcept(relationship.c2Uuid), defBuilder))
                        );
                    }
                }
            }
        });
        // process rel groups here...
        relGroupMap.forEachPair((group, assertionsInRelGroupList) -> {
            assertionList.add(
                    SomeRole(
                            Get.conceptService().getConcept(IsaacMetadataAuxiliaryBinding.ROLE_GROUP.getUuids()),
                            And(assertionsInRelGroupList.toArray(new Assertion[assertionsInRelGroupList.size()]))));
            return true;
        });
        return assertionList.toArray(new Assertion[assertionList.size()]);
    }

    private void removeDuplicates(SememeChronology<LogicGraphSememe<?>> logicChronology) {

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

        for (LogicGraphSememe graphToTest : sortedLogicGraphs) {
            if (lastGraph == null) {
                lastGraph = graphToTest;
                uniqueGraphs.add(graphToTest);
            } else {
                LogicalExpression lastExpression = parentTask.expressionBuilderService.fromSememe(lastGraph);
                LogicalExpression expressionToTest = parentTask.expressionBuilderService.fromSememe(graphToTest);
                if (Get.commitService().stampSequencesEqualExceptAuthorAndTime(
                        lastGraph.getStampSequence(), graphToTest.getStampSequence())
                        && !lastExpression.equals(expressionToTest)) {
                    lastGraph = graphToTest;
                    uniqueGraphs.add(graphToTest);
                }
            }
        }
        ((SememeChronologyImpl) logicChronology).setVersions(uniqueGraphs);
    }
}
