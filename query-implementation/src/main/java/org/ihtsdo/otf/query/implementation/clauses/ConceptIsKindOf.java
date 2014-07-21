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
import org.ihtsdo.otf.query.implementation.ClauseComputeType;
import org.ihtsdo.otf.query.implementation.LeafClause;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.api.store.Ts;
import org.ihtsdo.otf.tcc.api.concept.ConceptVersionBI;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.query.implementation.ClauseSemantic;
import org.ihtsdo.otf.query.implementation.WhereClause;
import org.ihtsdo.otf.tcc.api.spec.ConceptSpec;
import org.ihtsdo.otf.tcc.api.spec.ValidationException;

/**
 * Calculates the set of concepts that are a kind of the specified concept. The
 * calculated set is the union of the input concept and all concepts that lie
 * lie beneath the input concept in the terminology hierarchy.
 *
 * @author kec
 */
public class ConceptIsKindOf extends LeafClause {

    ConceptSpec kindOfSpec;
    String kindOfSpecKey;
    String viewCoordinateKey;
    ViewCoordinate viewCoordinate;
    Query enclosingQuery;

    public ConceptIsKindOf(Query enclosingQuery, String kindOfSpecKey, String viewCoordinateKey) {
        super(enclosingQuery);
        this.kindOfSpecKey = kindOfSpecKey;
        this.kindOfSpec = (ConceptSpec) enclosingQuery.getLetDeclarations().get(kindOfSpecKey);
        this.viewCoordinateKey = viewCoordinateKey;
        this.enclosingQuery = enclosingQuery;
    }

    @Override
    public NativeIdSetBI computePossibleComponents(NativeIdSetBI incomingPossibleComponents)
            throws ValidationException, IOException, ContradictionException {
        if (this.viewCoordinateKey.equals(this.enclosingQuery.currentViewCoordinateKey)) {
            this.viewCoordinate = (ViewCoordinate) this.enclosingQuery.getVCLetDeclarations().get(viewCoordinateKey);
        } else {
            this.viewCoordinate = (ViewCoordinate) this.enclosingQuery.getLetDeclarations().get(viewCoordinateKey);
        }
        int parentNid = kindOfSpec.getNid(this.viewCoordinate);
        getResultsCache().or(Ts.get().isKindOfSet(parentNid, this.viewCoordinate));

        return getResultsCache();
    }

    @Override
    public EnumSet<ClauseComputeType> getComputePhases() {
        return PRE_ITERATION;
    }

    @Override
    public void getQueryMatches(ConceptVersionBI conceptVersion) {
        // Nothing to do...
    }

    @Override
    public WhereClause getWhereClause() {
        WhereClause whereClause = new WhereClause();
        whereClause.setSemantic(ClauseSemantic.CONCEPT_IS_KIND_OF);
        whereClause.getLetKeys().add(kindOfSpecKey);
        whereClause.getLetKeys().add(viewCoordinateKey);
        return whereClause;
    }
}
