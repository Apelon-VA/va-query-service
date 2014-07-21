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
import org.ihtsdo.otf.query.implementation.LeafClause;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.query.implementation.WhereClause;
import org.ihtsdo.otf.tcc.api.concept.ConceptVersionBI;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.tcc.api.refex.RefexVersionBI;
import org.ihtsdo.otf.tcc.api.spec.ConceptSpec;
import org.ihtsdo.otf.tcc.api.spec.ValidationException;
import org.ihtsdo.otf.tcc.api.store.Ts;

/**
 * <code>LeafClause</code> that returns the nid of the input refset if a kind of
 * the input concept is a member of the refset and returns an empty set if a
 * kind of the input concept is not a member of the refset.
 *
 * @author dylangrald
 */
public class RefsetContainsKindOfConcept extends LeafClause {

    Query enclosingQuery;
    String refsetSpecKey;
    ConceptSpec refsetSpec;
    String conceptSpecKey;
    ConceptSpec conceptSpec;
    String viewCoordinateKey;
    ViewCoordinate viewCoordinate;

    public RefsetContainsKindOfConcept(Query enclosingQuery, String refsetSpecKey, String conceptSpecKey, String viewCoordinateKey) {
        super(enclosingQuery);
        this.enclosingQuery = enclosingQuery;
        this.refsetSpecKey = refsetSpecKey;
        this.refsetSpec = (ConceptSpec) this.enclosingQuery.getLetDeclarations().get(refsetSpecKey);
        this.conceptSpecKey = conceptSpecKey;
        this.conceptSpec = (ConceptSpec) this.enclosingQuery.getLetDeclarations().get(conceptSpecKey);
        this.viewCoordinateKey = viewCoordinateKey;

    }

    @Override
    public WhereClause getWhereClause() {
        WhereClause whereClause = new WhereClause();
        whereClause.setSemantic(ClauseSemantic.REFSET_CONTAINS_KIND_OF_CONCEPT);
        whereClause.getLetKeys().add(refsetSpecKey);
        whereClause.getLetKeys().add(conceptSpecKey);
        whereClause.getLetKeys().add(viewCoordinateKey);
        return whereClause;

    }

    @Override
    public EnumSet<ClauseComputeType> getComputePhases() {
        return PRE_ITERATION;
    }

    @Override
    public NativeIdSetBI computePossibleComponents(NativeIdSetBI incomingPossibleComponents) throws IOException, ValidationException, ContradictionException {
        if (this.viewCoordinateKey.equals(this.enclosingQuery.currentViewCoordinateKey)) {
            this.viewCoordinate = (ViewCoordinate) this.enclosingQuery.getVCLetDeclarations().get(viewCoordinateKey);
        } else {
            this.viewCoordinate = (ViewCoordinate) this.enclosingQuery.getLetDeclarations().get(viewCoordinateKey);
        }

        int parentNid = this.conceptSpec.getNid();
        NativeIdSetBI kindOfSet = Ts.get().isKindOfSet(parentNid, viewCoordinate);
        int refsetNid = this.refsetSpec.getNid();
        ConceptVersionBI conceptVersion = Ts.get().getConceptVersion(viewCoordinate, refsetNid);
        for (RefexVersionBI<?> rm : conceptVersion.getCurrentRefsetMembers(viewCoordinate)) {
            if (kindOfSet.contains(rm.getReferencedComponentNid())) {
                getResultsCache().add(refsetNid);
            }
        }

        return getResultsCache();
    }

    @Override
    public void getQueryMatches(ConceptVersionBI conceptVersion) throws IOException, ContradictionException {
        //Nothing to do here
    }
}
