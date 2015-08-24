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
package org.ihtsdo.otf.query.integration.tests;

import gov.vha.isaac.metadata.coordinates.StampCoordinates;
import gov.vha.isaac.metadata.coordinates.TaxonomyCoordinates;
import gov.vha.isaac.ochre.api.Get;
import java.io.IOException;
import org.ihtsdo.otf.query.implementation.Clause;
import org.ihtsdo.otf.query.implementation.ComponentCollectionTypes;
import org.ihtsdo.otf.query.implementation.ForSetSpecification;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;

/**
 * Creates a test for the <code>And</code> clause.
 *
 * @author dylangrald
 */
public class AndTest extends QueryClauseTest {

    public AndTest() throws IOException {
        this.q = new Query(TaxonomyCoordinates.getInferredTaxonomyCoordinate(StampCoordinates.getDevelopmentLatestActiveOnly(), 
                Get.configurationService().getDefaultLanguageCoordinate())) {
            @Override
            protected ForSetSpecification ForSetSpecification() {
                return new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
            }

            @Override
            public void Let() {
                let("Physical force", Snomed.PHYSICAL_FORCE);
                let("Motion", Snomed.MOTION);
            }

            @Override
            public Clause Where() {
                return And(ConceptIsKindOf("Physical force"),
                        Xor(ConceptIsKindOf("Motion"),
                                ConceptIsDescendentOf("Motion")));
            }
        };
    }
}
