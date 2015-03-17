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
package org.ihtsdo.otf.query.implementation.clauses;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import org.ihtsdo.otf.tcc.api.concept.ConceptVersionBI;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.nid.ConcurrentBitSet;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.query.implementation.Clause;
import org.ihtsdo.otf.query.implementation.ClauseComputeType;
import org.ihtsdo.otf.query.implementation.ClauseSemantic;
import org.ihtsdo.otf.query.implementation.ParentClause;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.query.implementation.WhereClause;
import org.ihtsdo.otf.tcc.api.spec.ValidationException;
import org.ihtsdo.otf.tcc.api.store.Ts;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Retrieves the preferred names for a result set of concepts.
 *
 * @author dylangrald
 */
@XmlRootElement
@XmlAccessorType(value = XmlAccessType.NONE)
public class PreferredNameForConcept extends ParentClause {

    public PreferredNameForConcept(Query enclosingQuery, Clause child) {
        super(enclosingQuery, child);

    }
    protected PreferredNameForConcept() {
    }
    @Override
    public WhereClause getWhereClause() {
        WhereClause whereClause = new WhereClause();
        whereClause.setSemantic(ClauseSemantic.PREFERRED_NAME_FOR_CONCEPT);
        for (Clause clause : getChildren()) {
            whereClause.getChildren().add(clause.getWhereClause());
        }
        return whereClause;
    }

    @Override
    public NativeIdSetBI computePossibleComponents(NativeIdSetBI incomingPossibleConcepts) throws IOException, ValidationException, ContradictionException {
        return incomingPossibleConcepts;
    }
    
    @Override
    public EnumSet<ClauseComputeType> getComputePhases(){
        return POST_ITERATION;
    }

    @Override
    public NativeIdSetBI computeComponents(NativeIdSetBI incomingConcepts) throws IOException, ValidationException, ContradictionException {
        ViewCoordinate viewCoordinate = getEnclosingQuery().getViewCoordinate();
        NativeIdSetBI outgoingPreferredNids = new ConcurrentBitSet();
        for (Clause childClause : getChildren()) {
            NativeIdSetBI childPossibleComponentNids = childClause.computePossibleComponents(incomingConcepts);
            Map<Integer, ConceptVersionBI> conceptMap = Ts.get().getConceptVersions(viewCoordinate, childPossibleComponentNids);
            for (Map.Entry<Integer, ConceptVersionBI> entry : conceptMap.entrySet()) {
                outgoingPreferredNids.add(entry.getValue().getPreferredDescription().getNid());
            }
        }
        return outgoingPreferredNids;
    }
}
