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
import org.ihtsdo.otf.query.implementation.Clause;
import org.ihtsdo.otf.query.implementation.ClauseComputeType;
import org.ihtsdo.otf.query.implementation.ClauseSemantic;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.query.implementation.ParentClause;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.tcc.api.nid.ConcurrentBitSet;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.query.implementation.WhereClause;
import org.ihtsdo.otf.tcc.api.store.Ts;
import org.ihtsdo.otf.tcc.api.spec.ValidationException;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Computes the set of enclosing concepts for the set of components that
 * are returned from the child clause.
 *
 * @author kec
 */
@XmlRootElement
@XmlAccessorType(value = XmlAccessType.NONE)
public class ConceptForComponent extends ParentClause {

    public ConceptForComponent(Query enclosingQuery, Clause child) {
        super(enclosingQuery, child);
    }
    protected ConceptForComponent() {
    }
    @Override
    public NativeIdSetBI computePossibleComponents(NativeIdSetBI incomingPossibleConceptNids) throws IOException, ValidationException, ContradictionException {
        NativeIdSetBI incomingPossibleComponentNids = Ts.get().getComponentNidsForConceptNids(incomingPossibleConceptNids);

        NativeIdSetBI outgoingPossibleConceptNids = new ConcurrentBitSet();
        for (Clause childClause : getChildren()) {
            NativeIdSetBI childPossibleComponentNids = childClause.computePossibleComponents(incomingPossibleComponentNids);
            outgoingPossibleConceptNids.or(Ts.get().getConceptNidsForComponentNids(childPossibleComponentNids));
        }
        return outgoingPossibleConceptNids;
    }
    
    @Override
    public EnumSet<ClauseComputeType> getComputePhases(){
        return POST_ITERATION;
    }

    @Override
    public WhereClause getWhereClause() {
        WhereClause whereClause = new WhereClause();
        whereClause.setSemantic(ClauseSemantic.CONCEPT_FOR_COMPONENT);
        for (Clause clause : getChildren()) {
            whereClause.getChildren().add(clause.getWhereClause());
        }
        return whereClause;
    }

    @Override
    public NativeIdSetBI computeComponents(NativeIdSetBI incomingComponents) throws IOException, ValidationException, ContradictionException {
        NativeIdSetBI incomingPossibleComponentNids = Ts.get().getComponentNidsForConceptNids(incomingComponents);
        NativeIdSetBI outgoingPossibleConceptNids = new ConcurrentBitSet();
        for (Clause childClause : getChildren()) {
            NativeIdSetBI childPossibleComponentNids = childClause.computeComponents(incomingPossibleComponentNids);
            outgoingPossibleConceptNids.or(Ts.get().getConceptNidsForComponentNids(childPossibleComponentNids));
        }
        return outgoingPossibleConceptNids;
    }
}
