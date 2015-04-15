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

import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import org.ihtsdo.otf.tcc.api.nid.IntSet;
import gov.vha.isaac.ochre.collections.NidSet;
import java.io.IOException;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.api.nid.ConcurrentBitSet;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.tcc.api.spec.ValidationException;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.api.concept.ConceptVersionBI;
import org.ihtsdo.otf.tcc.api.store.Ts;

/**
 * Returns components that are in the incoming For set and not in the set
 * returned from the computation of the clauses that are descendents of the
 * <code>Not</code> clause.
 *
 * @author kec
 */
@XmlRootElement()
public class Not extends ParentClause {

    @XmlTransient
    NativeIdSetBI forSet;
    @XmlTransient
    NativeIdSetBI notSet;

    public Not(Query enclosingQuery, Clause child) {
        super(enclosingQuery, child);
    }
    /**
     * Default no arg constructor for Jaxb.
     */
    protected Not() {
        super();
    }

    @Override
    public NativeIdSetBI computePossibleComponents(NativeIdSetBI incomingPossibleComponents) throws IOException, ValidationException, ContradictionException {
        this.notSet = new ConcurrentBitSet();
        for (Clause c : getChildren()) {
            for (ClauseComputeType cp : c.getComputePhases()) {
                switch (cp) {
                    case PRE_ITERATION:
                        notSet.or(c.computePossibleComponents(incomingPossibleComponents));
                        break;
                    case ITERATION:
                        c.computePossibleComponents(incomingPossibleComponents);
                        break;
                    case POST_ITERATION:
                        c.computePossibleComponents(incomingPossibleComponents);
                        break;
                }
            }
        }
        return incomingPossibleComponents;
    }

    @Override
    public WhereClause getWhereClause() {
        WhereClause whereClause = new WhereClause();
        whereClause.setSemantic(ClauseSemantic.NOT);
        for (Clause clause : getChildren()) {
            whereClause.getChildren().add(clause.getWhereClause());
        }
        return whereClause;
    }

    @Override
    public NativeIdSetBI computeComponents(NativeIdSetBI incomingComponents) throws IOException, ValidationException, ContradictionException {
        this.forSet = enclosingQuery.getForSet();
        assert forSet != null;
        ConceptSequenceSet activeSet = new ConceptSequenceSet();
        
        Ts.get().getConceptStream(incomingComponents.toConceptSequenceSet()).forEach((ConceptChronicleBI cc) -> {
            for (ConceptVersionBI cv: cc.getVersions(getEnclosingQuery().getViewCoordinate())) {
                try {
                    if (cv.isActive()) {
                        activeSet.add(cc.getNid());
                    }
                } catch (IOException ex) {
                   throw new RuntimeException(ex);
                }
            }
        });
        for (Clause c : getChildren()) {
            notSet.or(c.computeComponents(incomingComponents));
        }
        forSet = new IntSet(NidSet.of(activeSet).stream().toArray());
        
        forSet.andNot(notSet);
        return forSet;
    }
}
