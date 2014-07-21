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
import org.ihtsdo.otf.tcc.api.concept.ConceptVersionBI;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.nid.ConcurrentBitSet;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.query.implementation.ClauseComputeType;
import org.ihtsdo.otf.query.implementation.ClauseSemantic;
import org.ihtsdo.otf.query.implementation.LeafClause;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.query.implementation.WhereClause;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetItrBI;
import org.ihtsdo.otf.tcc.api.relationship.RelationshipChronicleBI;
import org.ihtsdo.otf.tcc.api.relationship.RelationshipVersionBI;
import org.ihtsdo.otf.tcc.api.spec.ConceptSpec;
import org.ihtsdo.otf.tcc.api.spec.ValidationException;
import org.ihtsdo.otf.tcc.api.store.Ts;
import org.ihtsdo.otf.tcc.datastore.Bdb;

/**
 * Computes all concepts that have a source relationship matching the input
 * destination concept and relationship type. If the relationship type
 * subsumption is true, then the clause computes the matching concepts using all
 * relationship types that are a kind of the input relationship type. Queries
 * that specify a relationship destination restriction can be constructed using
 * the {@link org.ihtsdo.otf.query.implementation.clauses.RelRestriction} clause.
 *
 * @author dylangrald
 */
public class RelType extends LeafClause {

    ConceptSpec destinationSpec;
    String destinationSpecKey;
    String viewCoordinateKey;
    ViewCoordinate viewCoordinate;
    Query enclosingQuery;
    ConceptSpec relType;
    String relTypeSpecKey;
    NativeIdSetBI cache;
    Boolean relTypeSubsumption;
    boolean destinationSubsumption;

    public RelType(Query enclosingQuery, String relTypeSpecKey, String destinationSpecKey, String viewCoordinateKey, Boolean relTypeSubsumption) {
        this(enclosingQuery, relTypeSpecKey, destinationSpecKey, viewCoordinateKey, relTypeSubsumption, true);
    }

    public RelType(Query enclosingQuery, String relTypeSpecKey, String destinationSpecKey, String viewCoordinateKey, Boolean relTypeSubsumption, boolean destinationSubsumption) {
        super(enclosingQuery);
        this.destinationSpecKey = destinationSpecKey;
        this.destinationSpec = (ConceptSpec) enclosingQuery.getLetDeclarations().get(destinationSpecKey);
        this.viewCoordinateKey = viewCoordinateKey;
        this.enclosingQuery = enclosingQuery;
        this.relTypeSpecKey = relTypeSpecKey;
        this.relType = (ConceptSpec) enclosingQuery.getLetDeclarations().get(relTypeSpecKey);
        this.relTypeSubsumption = relTypeSubsumption;
        this.destinationSubsumption = destinationSubsumption;
    }

    @Override
    public WhereClause getWhereClause() {
        WhereClause whereClause = new WhereClause();
        whereClause.setSemantic(ClauseSemantic.REL_TYPE);
        whereClause.getLetKeys().add(relTypeSpecKey);
        whereClause.getLetKeys().add(destinationSpecKey);
        whereClause.getLetKeys().add(viewCoordinateKey);
        return whereClause;
    }

    @Override
    public EnumSet<ClauseComputeType> getComputePhases() {
        return PRE_AND_POST_ITERATION;
    }

    @Override
    public NativeIdSetBI computePossibleComponents(NativeIdSetBI incomingPossibleComponents) throws IOException, ValidationException, ContradictionException {
        if (this.viewCoordinateKey.equals(this.enclosingQuery.currentViewCoordinateKey)) {
            this.viewCoordinate = (ViewCoordinate) this.enclosingQuery.getVCLetDeclarations().get(viewCoordinateKey);
        } else {
            this.viewCoordinate = (ViewCoordinate) this.enclosingQuery.getLetDeclarations().get(viewCoordinateKey);
        }
        NativeIdSetBI relTypeSet = new ConcurrentBitSet();
        relTypeSet.add(this.relType.getNid());
        if (this.relTypeSubsumption) {
            relTypeSet.or(Ts.get().isKindOfSet(this.relType.getNid(), viewCoordinate));
        }
        NativeIdSetBI destinationIdSet = new ConcurrentBitSet();
        destinationIdSet.add(this.destinationSpec.getNid());
        if (this.destinationSubsumption) {
            destinationIdSet.or(Ts.get().isKindOfSet(this.destinationSpec.getNid(), viewCoordinate));
        }
        NativeIdSetItrBI iter = destinationIdSet.getSetBitIterator();
        while (iter.next()) {
            int[] destRelNids = Bdb.getMemoryCache().getDestRelNids(iter.nid());
            for (int i : destRelNids) {
                RelationshipChronicleBI rel = (RelationshipChronicleBI) Ts.get().getComponent(i);
                RelationshipVersionBI relVersion = rel.getVersion(viewCoordinate);
                if (relVersion != null) {
                    if (relTypeSet.contains(relVersion.getTypeNid())) {
                        this.addToResultsCache(rel.getConceptNid());
                    }
                }
            }
        }

        return getResultsCache();
    }

    @Override
    public void getQueryMatches(ConceptVersionBI conceptVersion) throws IOException, ContradictionException {
        //Nothing to do here...
    }
}
