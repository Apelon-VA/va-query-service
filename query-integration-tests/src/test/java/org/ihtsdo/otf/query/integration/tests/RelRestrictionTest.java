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
import gov.vha.isaac.metadata.coordinates.ViewCoordinates;
import gov.vha.isaac.ochre.api.Get;
import java.io.IOException;
import java.util.EnumSet;
import org.ihtsdo.otf.query.implementation.Clause;
import org.ihtsdo.otf.query.implementation.ComponentCollectionTypes;
import org.ihtsdo.otf.query.implementation.ForSetSpecification;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;

/**
 * Creates a test for the <code>RelRestriction</code> clause.
 *
 * @author dylangrald
 */
public class RelRestrictionTest extends QueryClauseTest {

    public RelRestrictionTest() throws IOException {
        this.q = new Query(TaxonomyCoordinates.getInferredTaxonomyCoordinate(StampCoordinates.getDevelopmentLatestActiveOnly(), 
                Get.configurationService().getDefaultLanguageCoordinate())) {
            @Override
            protected ForSetSpecification ForSetSpecification() {
                return new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
            }
            @Override
            public void Let() {
                let("Procedure site", Snomed.PROCEDURE_SITE);
                let("Eye structure", Snomed.EYE_STRUCTURE);
                let("Laser surgery", Snomed.LASER_SURGERY);
                let("relTypeSubsumptionKey", true);
                let("destinationSubsumptionKey", true);
            }

            @Override
            public Clause Where() {
                return Or(RelRestriction("Procedure site", "Laser surgery", "relTypeSubsumptionKey", "destinationSubsumptionKey"));
            }
        };
    }
}
