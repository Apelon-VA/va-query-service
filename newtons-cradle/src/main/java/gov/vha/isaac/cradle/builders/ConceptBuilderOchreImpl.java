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
package gov.vha.isaac.cradle.builders;

import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.ConceptProxy;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.State;
import gov.vha.isaac.ochre.api.commit.ChangeCheckerMode;
import gov.vha.isaac.ochre.api.component.concept.ConceptBuilder;
import gov.vha.isaac.ochre.api.component.concept.ConceptChronology;
import gov.vha.isaac.ochre.api.component.concept.description.DescriptionBuilder;
import gov.vha.isaac.ochre.api.component.concept.description.DescriptionBuilderService;
import gov.vha.isaac.ochre.api.component.sememe.SememeBuilder;
import gov.vha.isaac.ochre.api.component.sememe.SememeBuilderService;
import gov.vha.isaac.ochre.api.coordinate.EditCoordinate;
import gov.vha.isaac.ochre.api.coordinate.LogicCoordinate;
import gov.vha.isaac.ochre.api.logic.LogicalExpression;
import gov.vha.isaac.ochre.model.concept.ConceptChronologyImpl;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author kec
 */
public class ConceptBuilderOchreImpl extends ComponentBuilder<ConceptChronology> implements ConceptBuilder {

    private final String conceptName;
    private final String semanticTag;
    private final ConceptProxy defaultLanguageForDescriptions;
    private final ConceptProxy defaultDialectAssemblageForDescriptions;
    private final LogicCoordinate defaultLogicCoordinate;
    private final LogicalExpression logicalExpression;
    private final List<DescriptionBuilder> descriptionBuilders = new ArrayList<>();
    private final List<SememeBuilder> logicalConceptDefinitionBuilders = new ArrayList<>();

    public ConceptBuilderOchreImpl(String conceptName,
            String semanticTag,
            LogicalExpression logicalExpression,
            ConceptProxy defaultLanguageForDescriptions,
            ConceptProxy defaultDialectAssemblageForDescriptions,
            LogicCoordinate defaultLogicCoordinate) {
        this.conceptName = conceptName;
        this.semanticTag = semanticTag;
        this.defaultLanguageForDescriptions = defaultLanguageForDescriptions;
        this.defaultDialectAssemblageForDescriptions = defaultDialectAssemblageForDescriptions;
        this.defaultLogicCoordinate = defaultLogicCoordinate;
        this.logicalExpression = logicalExpression;
    }

    @Override
    public DescriptionBuilder getFullySpecifiedDescriptionBuilder() {
        StringBuilder descriptionTextBuilder = new StringBuilder();
        descriptionTextBuilder.append(conceptName);
        if (semanticTag != null && semanticTag.length() > 0) {
            descriptionTextBuilder.append(" (");
            descriptionTextBuilder.append(semanticTag);
            descriptionTextBuilder.append(")");
        }
        return LookupService.getService(DescriptionBuilderService.class).
                getDescriptionBuilder(descriptionTextBuilder.toString(), this,
                        IsaacMetadataAuxiliaryBinding.FULLY_SPECIFIED_NAME,
                        defaultLanguageForDescriptions).
                setPreferredInDialectAssemblage(defaultDialectAssemblageForDescriptions);
    }

    @Override
    public DescriptionBuilder getPreferredDescriptionBuilder() {
        return LookupService.getService(DescriptionBuilderService.class).
                getDescriptionBuilder(conceptName, this,
                        IsaacMetadataAuxiliaryBinding.PREFERRED,
                        defaultLanguageForDescriptions).
                setPreferredInDialectAssemblage(defaultDialectAssemblageForDescriptions);
    }

    @Override
    public ConceptBuilder addDescription(DescriptionBuilder descriptionBuilder) {
        descriptionBuilders.add(descriptionBuilder);
        return this;
    }

    @Override
    public ConceptBuilder addLogicalDefinition(SememeBuilder logicalDefinitionBuilder) {
        this.logicalConceptDefinitionBuilders.add(logicalDefinitionBuilder);
        return this;
    }

    @Override
    public ConceptChronology build(EditCoordinate editCoordinate, ChangeCheckerMode changeCheckerMode,
            List builtObjects) throws IllegalStateException {

        ConceptChronologyImpl conceptChronology
                = (ConceptChronologyImpl) getConceptService().getConcept(getUuids());
        conceptChronology.createMutableVersion(State.ACTIVE, editCoordinate);
        builtObjects.add(conceptChronology);
        descriptionBuilders.add(getFullySpecifiedDescriptionBuilder());
        descriptionBuilders.add(getPreferredDescriptionBuilder());
        descriptionBuilders.forEach((builder) -> {
            builder.build(editCoordinate, changeCheckerMode, builtObjects);
        });
        SememeBuilderService builderService = LookupService.getService(SememeBuilderService.class);
        logicalConceptDefinitionBuilders.add(builderService.
                getLogicalExpressionSememeBuilder(logicalExpression, this, defaultLogicCoordinate.getStatedAssemblageSequence()));
        logicalConceptDefinitionBuilders.forEach((builder) -> builder.build(editCoordinate, changeCheckerMode, builtObjects));
        return conceptChronology;
    }

    @Override
    public ConceptChronology build(int stampCoordinate,
            List builtObjects) throws IllegalStateException {

        ConceptChronologyImpl conceptChronology
                = (ConceptChronologyImpl) getConceptService().getConcept(getUuids());
        conceptChronology.createMutableVersion(stampCoordinate);
        builtObjects.add(conceptChronology);
        descriptionBuilders.add(getFullySpecifiedDescriptionBuilder());
        descriptionBuilders.add(getPreferredDescriptionBuilder());
        descriptionBuilders.forEach((builder) -> {
            builder.build(stampCoordinate, builtObjects);
        });
        SememeBuilderService builderService = LookupService.getService(SememeBuilderService.class);
        logicalConceptDefinitionBuilders.add(builderService.
                getLogicalExpressionSememeBuilder(logicalExpression, this, defaultLogicCoordinate.getStatedAssemblageSequence()));
        logicalConceptDefinitionBuilders.forEach((builder) -> builder.build(stampCoordinate, builtObjects));
        return conceptChronology;
    }
}
