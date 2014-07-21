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
import java.io.IOException;
import java.util.EnumSet;
import org.ihtsdo.otf.query.implementation.Clause;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.tcc.api.coordinate.StandardViewCoordinates;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.tcc.api.store.Ts;

/**
 * Creates a test for
 * {@link org.ihtsdo.otf.query.implementation.clauses.ConceptIsKindOf} clause.
 *
 * @author dylangrald
 */
public class IsKindOfTest extends QueryClauseTest {

    public IsKindOfTest() throws IOException {
        ViewCoordinate vc = StandardViewCoordinates.getSnomedInferredLatestActiveOnly();
        vc.setAllowedStatus(EnumSet.of(Status.ACTIVE));
        this.q = new Query(vc) {
            @Override
            protected NativeIdSetBI For() throws IOException {
                return Ts.get().getAllConceptNids();
            }

            @Override
            public void Let() throws IOException {
                let("Physical force", Snomed.PHYSICAL_FORCE);
            }

            @Override
            public Clause Where() {
                return And(ConceptIsKindOf("Physical force"));
            }
        };
    }
}
