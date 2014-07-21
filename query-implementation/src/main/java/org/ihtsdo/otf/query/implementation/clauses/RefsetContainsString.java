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

/**
 *
 * @author dylangrald
 */
import java.io.IOException;
import java.util.EnumSet;
import org.ihtsdo.otf.query.implementation.ClauseComputeType;
import org.ihtsdo.otf.query.implementation.ClauseSemantic;
import org.ihtsdo.otf.query.implementation.LeafClause;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.query.implementation.WhereClause;
import org.ihtsdo.otf.tcc.api.concept.ConceptVersionBI;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import static org.ihtsdo.otf.tcc.api.refex.RefexType.CID_CID_CID_STRING;
import static org.ihtsdo.otf.tcc.api.refex.RefexType.CID_CID_STR;
import static org.ihtsdo.otf.tcc.api.refex.RefexType.CID_STR;
import static org.ihtsdo.otf.tcc.api.refex.RefexType.STR;
import org.ihtsdo.otf.tcc.api.refex.RefexVersionBI;
import org.ihtsdo.otf.tcc.api.refex.type_string.RefexStringVersionBI;
import org.ihtsdo.otf.tcc.api.spec.ConceptSpec;
import org.ihtsdo.otf.tcc.api.spec.ValidationException;
import org.ihtsdo.otf.tcc.api.store.Ts;

/**
 * .
 *
 * @author dylangrald
 */
public class RefsetContainsString extends LeafClause {

    Query enclosingQuery;
    String queryText;
    String viewCoordinateKey;
    ViewCoordinate viewCoordinate;
    NativeIdSetBI cache;
    ConceptSpec refsetSpec;
    String refsetSpecKey;

    public RefsetContainsString(Query enclosingQuery, String refsetSpecKey, String queryText, String viewCoordinateKey) {
        super(enclosingQuery);
        this.enclosingQuery = enclosingQuery;
        this.refsetSpecKey = refsetSpecKey;
        this.refsetSpec = (ConceptSpec) this.enclosingQuery.getLetDeclarations().get(refsetSpecKey);
        this.queryText = queryText;
        this.viewCoordinateKey = viewCoordinateKey;

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
        int refsetNid = this.refsetSpec.getNid();
        ConceptVersionBI conceptVersion = Ts.get().getConceptVersion(viewCoordinate, refsetNid);

        for (RefexVersionBI<?> rm : conceptVersion.getCurrentRefsetMembers(viewCoordinate)) {
            switch (rm.getRefexType()) {
                case CID_STR:
                case CID_CID_CID_STRING:
                case CID_CID_STR:
                case STR:
                    RefexStringVersionBI rsv = (RefexStringVersionBI) rm;
                    if (rsv.getString1().toLowerCase().contains(queryText.toLowerCase())) {
                        getResultsCache().add(refsetNid);
                    }
                default:
                //do nothing

            }
        }

        return getResultsCache();
    }

    @Override
    public void getQueryMatches(ConceptVersionBI conceptVersion) throws IOException, ContradictionException {

    }

    @Override
    public WhereClause getWhereClause() {
        WhereClause whereClause = new WhereClause();
        whereClause.setSemantic(ClauseSemantic.REFSET_CONTAINS_STRING);
        whereClause.getLetKeys().add(refsetSpecKey);
        whereClause.getLetKeys().add(queryText);
        whereClause.getLetKeys().add(viewCoordinateKey);
        return whereClause;
    }
}
