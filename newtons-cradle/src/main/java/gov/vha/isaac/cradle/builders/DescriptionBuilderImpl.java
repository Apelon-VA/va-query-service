/*
 * Copyright 2015 kec.
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
package gov.vha.isaac.cradle.builders;

import gov.vha.isaac.ochre.api.commit.ChangeCheckerMode;
import gov.vha.isaac.ochre.api.ConceptProxy;
import gov.vha.isaac.ochre.api.State;
import gov.vha.isaac.ochre.api.chronicle.ChronicledObjectLocal;
import gov.vha.isaac.ochre.api.concept.ConceptBuilder;
import gov.vha.isaac.ochre.api.coordinate.EditCoordinate;
import gov.vha.isaac.metadata.coordinates.LanguageCoordinates;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.description.DescriptionBuilder;
import gov.vha.isaac.ochre.api.sememe.SememeBuilderService;
import java.beans.PropertyVetoException;
import java.util.ArrayList;
import java.util.List;
import org.ihtsdo.otf.tcc.model.cc.description.Description;

/**
 *
 * @author kec
 */
public class DescriptionBuilderImpl extends ComponentBuilder<ChronicledObjectLocal> implements DescriptionBuilder {
    
    
    private final ArrayList<ConceptProxy> preferredInDialectAssemblages = new ArrayList<>();
    private final ArrayList<ConceptProxy> acceptableInDialectAssemblages = new ArrayList<>();
    
    private final String descriptionText;
    private final ConceptProxy descriptionType;
    private final ConceptProxy languageForDescription;
    private final ConceptBuilder conceptBuilder;
    private int conceptSequence = Integer.MAX_VALUE;

    public DescriptionBuilderImpl(String descriptionText, 
            int conceptSequence,
            ConceptProxy descriptionType, 
            ConceptProxy languageForDescription) {
        this.descriptionText = descriptionText;
        this.conceptSequence = conceptSequence;
        this.descriptionType = descriptionType;
        this.languageForDescription = languageForDescription;
        this.conceptBuilder = null;
    }
    public DescriptionBuilderImpl(String descriptionText, 
            ConceptBuilder conceptBuilder,
            ConceptProxy descriptionType, 
            ConceptProxy languageForDescription) {
        this.descriptionText = descriptionText;
        this.descriptionType = descriptionType;
        this.languageForDescription = languageForDescription;
        this.conceptBuilder = conceptBuilder;
    }

    @Override
    public DescriptionBuilder setPreferredInDialectAssemblage(ConceptProxy dialectAssemblage) {
        preferredInDialectAssemblages.add(dialectAssemblage);
        return this; 
   }

    @Override
    public DescriptionBuilder setAcceptableInDialectAssemblage(ConceptProxy dialectAssemblage) {
        acceptableInDialectAssemblages.add(dialectAssemblage);
        return this;
    }

    @Override
    public Description build(EditCoordinate editCoordinate, ChangeCheckerMode changeCheckerMode,
            List builtObjects) throws IllegalStateException {
        try {
            if (conceptSequence == Integer.MAX_VALUE) {
                conceptSequence = getIdentifierService().getConceptSequenceForUuids(conceptBuilder.getUuids());
            }
            
            Description desc = new Description();
            desc.setSTAMP(getCommitService().getStamp(State.ACTIVE, Long.MAX_VALUE,
                    editCoordinate.getAuthorSequence(), editCoordinate.getModuleSequence(),
                    editCoordinate.getPathSequence()));
            desc.setPrimordialUuid(primordialUuid);
            desc.setNid(getIdentifierService().getNidForUuids(this.getUuids()));
            getIdentifierService().setConceptSequenceForComponentNid(conceptSequence, desc.nid);
            desc.enclosingConceptNid = getIdentifierService().getConceptNid(conceptSequence);
            desc.setText(descriptionText);
            desc.setTypeNid(getIdentifierService().getNidForProxy(descriptionType));
            desc.setInitialCaseSignificant(false);
            desc.setLang(LanguageCoordinates.conceptNidToIso639(getIdentifierService().getNidForProxy(languageForDescription)));
            desc.setAdditionalUuids(additionalUuids);

            SememeBuilderService sememeBuilderService = LookupService.getService(SememeBuilderService.class);
            
            preferredInDialectAssemblages.forEach(( assemblageProxy) -> {
                sememeBuilderService.getConceptSememeBuilder(
                        IsaacMetadataAuxiliaryBinding.PREFERRED, this, 
                        getIdentifierService().getConceptSequenceForProxy(assemblageProxy)).
                        build(editCoordinate, changeCheckerMode, builtObjects);
            });
            acceptableInDialectAssemblages.forEach(( assemblageProxy) -> {
                sememeBuilderService.getConceptSememeBuilder(
                        IsaacMetadataAuxiliaryBinding.ACCEPTABLE, this, 
                        getIdentifierService().getConceptSequenceForProxy(assemblageProxy)).
                        build(editCoordinate, changeCheckerMode, builtObjects);
            });
            builtObjects.add(desc);
            return desc;
        } catch (PropertyVetoException ex) {
            throw new RuntimeException(ex);
        }
    }
    
}
