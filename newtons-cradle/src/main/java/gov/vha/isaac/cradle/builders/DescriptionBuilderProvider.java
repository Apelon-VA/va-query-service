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

import gov.vha.isaac.ochre.api.ConceptModel;
import gov.vha.isaac.ochre.api.ConceptProxy;
import gov.vha.isaac.ochre.api.ConfigurationService;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.component.concept.ConceptBuilder;
import gov.vha.isaac.ochre.api.component.concept.description.DescriptionBuilder;
import gov.vha.isaac.ochre.api.component.concept.description.DescriptionBuilderService;
import org.jvnet.hk2.annotations.Service;

/**
 *
 * @author kec
 */
@Service
public class DescriptionBuilderProvider implements DescriptionBuilderService {

    private static ConceptModel conceptModel;
    private static ConceptModel getConceptModel() {
        if (conceptModel == null) {
            conceptModel = LookupService.getService(ConfigurationService.class).getConceptModel();
        }
        return conceptModel;
    }

    @Override
    public DescriptionBuilder getDescriptionBuilder(String descriptionText, int conceptSequence, ConceptProxy descriptionType, ConceptProxy languageForDescription) {
        switch (getConceptModel()) {
            case OCHRE_CONCEPT_MODEL:
                return new DescriptionBuilderOchreImpl(descriptionText, conceptSequence, descriptionType, languageForDescription);
            default:
                throw new UnsupportedOperationException("Can't handle: " + conceptModel);
        }
    }

    @Override
    public DescriptionBuilder getDescriptionBuilder(String descriptionText, ConceptBuilder conceptBuilder, ConceptProxy descriptionType, ConceptProxy languageForDescription) {
        switch (getConceptModel()) {
            case OCHRE_CONCEPT_MODEL:
                return new DescriptionBuilderOchreImpl(descriptionText, conceptBuilder, descriptionType, languageForDescription);
            default:
                throw new UnsupportedOperationException("Can't handle: " + conceptModel);
        }
    }

}
