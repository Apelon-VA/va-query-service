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
package org.ihtsdo.otf.query.implementation;

import gov.vha.isaac.metadata.coordinates.ViewCoordinates;
import gov.vha.isaac.ochre.collections.NidSet;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;

/**
 * Demonstrates the syntax to construct and compute a
 * <code>Query</code>.
 *
 * @author kec
 */
public class QueryExample {

    Query q;
    
    public QueryExample(){
        this.q = new Query(ViewCoordinates.getDevelopmentInferredLatestActiveOnly()) {
            @Override
            protected ForSetSpecification ForSetSpecification() {
                return new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
            }
            
            @Override
            public void Let()  {
                let("allergic-asthma", Snomed.ALLERGIC_ASTHMA);
                let("asthma", Snomed.ASTHMA);
                let("mild asthma", Snomed.MILD_ASTHMA);
            }
            
            @Override
            public Clause Where() {
                return And(ConceptIsKindOf("asthma"),
                        Not(ConceptIsChildOf("allergic-asthma")),
                        ConceptIs("allergic-asthma"));
//                                Union(ConceptIsKindOf("allergic-asthma"),
//                                ConceptIsKindOf("mild asthma")));
            }
        };
    }
    
    public NidSet getResults() throws IOException, Exception{
        return this.q.compute();
    }
}
