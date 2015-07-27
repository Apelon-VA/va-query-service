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

import gov.vha.isaac.ochre.api.Get;
import gov.vha.isaac.ochre.api.coordinate.CoordinateFactory;
import gov.vha.isaac.ochre.api.coordinate.TaxonomyCoordinate;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.ihtsdo.otf.query.implementation.Clause;
import org.ihtsdo.otf.query.implementation.ComponentCollectionTypes;
import org.ihtsdo.otf.query.implementation.ForSetSpecification;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;

/**
 * Creates a test for the {@link org.ihtsdo.otf.query.implementation.Xor}
 * clause. Demonstrates the ability to do difference queries.
 *
 * @author dylangrald
 */
public class XorVersionTest extends QueryClauseTest {
    
    TaxonomyCoordinate taxonomyCoordinateV1;
    TaxonomyCoordinate taxonomyCoordinateV2;
    
    public XorVersionTest() throws IOException {
        CoordinateFactory factory = Get.coordinateFactory();
        
        this.taxonomyCoordinateV1 = Get.coordinateFactory().createInferredTaxonomyCoordinate(
                factory.createDevelopmentLatestActiveOnlyStampCoordinate().makeAnalog(2002, 1, 31, 0, 0, 0), 
                factory.getUsEnglishLanguageFullySpecifiedNameCoordinate(), 
                factory.createStandardElProfileLogicCoordinate());
 
        this.taxonomyCoordinateV2 = Get.coordinateFactory().createInferredTaxonomyCoordinate(
                factory.createDevelopmentLatestActiveOnlyStampCoordinate(), 
                factory.getUsEnglishLanguageFullySpecifiedNameCoordinate(), 
                factory.createStandardElProfileLogicCoordinate());
 
        Logger.getLogger(XorVersionTest.class.getName()).log(Level.INFO, 
                "ViewCoordinate in XorVersionTest: {0}", this.taxonomyCoordinateV1);
        this.q = new Query(taxonomyCoordinateV2) {
            @Override
            protected ForSetSpecification ForSetSpecification() {
                return new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
            }

            @Override
            public void Let() {
                let("disease", Snomed.DISEASE);
                let("v2", taxonomyCoordinateV2);
            }
            
            @Override
            public Clause Where() {
                return Xor(ConceptIsKindOf("disease"), ConceptIsKindOf("disease", "v2"));
            }
        };
        
    }
}
