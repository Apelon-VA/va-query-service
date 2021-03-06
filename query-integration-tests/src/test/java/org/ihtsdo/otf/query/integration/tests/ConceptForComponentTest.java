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
import gov.vha.isaac.metadata.coordinates.StampCoordinates;
import gov.vha.isaac.metadata.coordinates.TaxonomyCoordinates;
import gov.vha.isaac.metadata.coordinates.ViewCoordinates;
import gov.vha.isaac.ochre.api.Get;
import java.io.IOException;
import org.ihtsdo.otf.query.implementation.Clause;
import org.ihtsdo.otf.query.implementation.ComponentCollectionTypes;
import org.ihtsdo.otf.query.implementation.ForSetSpecification;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;
import org.ihtsdo.otf.tcc.api.nid.ConcurrentBitSet;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.tcc.model.cc.PersistentStore;

/**
 * Creates a test for the
 * <code>ConceptForComponent</code> clause.
 *
 * @author dylangrald
 */
public class ConceptForComponentTest extends QueryClauseTest {


    public ConceptForComponentTest() throws IOException {
        this.q = new Query(TaxonomyCoordinates.getInferredTaxonomyCoordinate(StampCoordinates.getDevelopmentLatestActiveOnly(), 
                Get.configurationService().getDefaultLanguageCoordinate())) {
            @Override
            protected ForSetSpecification ForSetSpecification() {
                try {
                    ForSetSpecification forSetSpecification = new ForSetSpecification(ComponentCollectionTypes.CUSTOM_SET);
                    NativeIdSetBI forSet = new ConcurrentBitSet();
                    forSet.or(PersistentStore.get().isKindOfSet(Snomed.MOTION.getNid(), ViewCoordinates.getDevelopmentInferredLatestActiveOnly()));
                    forSetSpecification.getCustomCollection().addAll(forSet.toPrimordialUuidSet());
                    return forSetSpecification;
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }

            @Override
            public void Let() {
                let("motion", Snomed.MOTION);
                let("regex", ".*tion.*");
            }

            @Override
            public Clause Where() {
                return Intersection(ConceptIsChildOf("motion"),
                            ConceptForComponent(DescriptionRegexMatch("regex")));
            }
        };
    }
}
