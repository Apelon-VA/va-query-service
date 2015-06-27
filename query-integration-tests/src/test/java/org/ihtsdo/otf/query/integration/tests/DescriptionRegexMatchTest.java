package org.ihtsdo.otf.query.integration.tests;

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


import gov.vha.isaac.metadata.coordinates.ViewCoordinates;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.ihtsdo.otf.query.implementation.ComponentCollectionTypes;
import org.ihtsdo.otf.query.implementation.ForSetSpecification;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;
import org.ihtsdo.otf.tcc.api.nid.ConcurrentBitSet;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.query.implementation.Clause;
import org.ihtsdo.otf.query.implementation.Query;

/**
 * Creates a test for the
 * <code>DescriptionRegexMatch</code> clause.
 *
 * @author dylangrald
 */
public class DescriptionRegexMatchTest extends QueryClauseTest {


    public DescriptionRegexMatchTest() throws IOException {
        this.q = new Query(ViewCoordinates.getDevelopmentInferredLatestActiveOnly()) {

            @Override
            protected ForSetSpecification ForSetSpecification() {
                try {
                    ForSetSpecification forSetSpecification = new ForSetSpecification(ComponentCollectionTypes.CUSTOM_SET);
                    NativeIdSetBI forSet = new ConcurrentBitSet();
                    forSet.add(Snomed.MOTION.getNid());
                    forSet.add(Snomed.ACCELERATION.getNid());
                    forSet.add(Snomed.CENTRIFUGAL_FORCE.getNid());
                    forSet.add(Snomed.CONTINUED_MOVEMENT.getNid());
                    forSet.add(Snomed.DECELERATION.getNid());
                    forSet.add((Snomed.MOMENTUM.getNid()));
                    forSet.add(Snomed.VIBRATION.getNid());
                    forSetSpecification.getCustomCollection().addAll(forSet.toPrimordialUuidSet());
                    return forSetSpecification;
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }

            @Override
            public void Let() {
                let("motion", Snomed.MOTION);
                let("deceleration", "[Dd]eceleration.*");
                let("vibration", "[Vv]ibration.*");
            }

            @Override
            public Clause Where() {
                return Or(ConceptForComponent(DescriptionRegexMatch("deceleration")), 
                          ConceptForComponent(DescriptionRegexMatch("vibration")));
            }
        };

    }

}
