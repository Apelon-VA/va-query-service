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
import org.ihtsdo.otf.query.implementation.ClauseSemantic;
import org.ihtsdo.otf.query.implementation.LeafClause;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.query.implementation.WhereClause;
import org.ihtsdo.otf.tcc.api.concept.ConceptVersionBI;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.nid.ConcurrentBitSet;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetItrBI;
import org.ihtsdo.otf.tcc.api.relationship.RelationshipChronicleBI;
import org.ihtsdo.otf.tcc.api.relationship.RelationshipVersionBI;
import org.ihtsdo.otf.tcc.api.spec.ConceptSpec;
import org.ihtsdo.otf.tcc.api.spec.ValidationException;
import org.ihtsdo.otf.tcc.api.store.Ts;
import org.ihtsdo.otf.tcc.datastore.Bdb;
/**
 * Allows the user to define a restriction on the destination set of a
 * relationship query. Also allows the user to specify subsumption on the
 * destination restriction and relType.
 *
 * @author dylangrald
 */
public class RelRestriction extends LeafClause {

    ViewCoordinate viewCoordinate;
    Query enclosingQuery;
    String relTypeKey;
    ConceptSpec relType;
    String destinationSpecKey;
    ConceptSpec destinationSpec;
    String relRestrictionSpecKey;
    ConceptSpec relRestrictionSpec;
    String viewCoordinateKey;
    String destinationSubsumptionKey;
    Boolean destinationSubsumption;
    String relTypeSubsumptionKey;
    Boolean relTypeSubsumption;

    public RelRestriction(Query enclosingQuery, String relRestrictionSpecKey, String relTypeKey, String destinationSpecKey,
            String viewCoordinateKey, String destinationSubsumptionKey, String relTypeSubsumptionKey) {
        super(enclosingQuery);
        this.enclosingQuery = enclosingQuery;
        this.destinationSpecKey = destinationSpecKey;
        this.destinationSpec = (ConceptSpec) enclosingQuery.getLetDeclarations().get(destinationSpecKey);
        this.relTypeKey = relTypeKey;
        this.relType = (ConceptSpec) enclosingQuery.getLetDeclarations().get(relTypeKey);
        this.relRestrictionSpecKey = relRestrictionSpecKey;
        this.relRestrictionSpec = (ConceptSpec) enclosingQuery.getLetDeclarations().get(relRestrictionSpecKey);
        this.viewCoordinateKey = viewCoordinateKey;
        this.relTypeSubsumptionKey = relTypeSubsumptionKey;
        this.destinationSubsumptionKey = destinationSubsumptionKey;
        this.relTypeSubsumption = (Boolean) enclosingQuery.getLetDeclarations().get(relTypeSubsumptionKey);
        this.destinationSubsumption = (Boolean) enclosingQuery.getLetDeclarations().get(destinationSubsumptionKey);

    }

    @Override
    public WhereClause getWhereClause() {
        WhereClause whereClause = new WhereClause();
        whereClause.setSemantic(ClauseSemantic.REL_RESTRICTION);
        whereClause.getLetKeys().add(relRestrictionSpecKey);
        whereClause.getLetKeys().add(relTypeKey);
        whereClause.getLetKeys().add(destinationSpecKey);
        whereClause.getLetKeys().add(viewCoordinateKey);
        whereClause.getLetKeys().add(destinationSubsumptionKey);
        whereClause.getLetKeys().add(relTypeSubsumptionKey);
        System.out.println("Where clause size: " + whereClause.getLetKeys().size());
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
        //The default is to set relTypeSubsumption and destinationSubsumption to true.
        if (this.relTypeSubsumption == null) {
            this.relTypeSubsumption = true;
        }
        if (this.destinationSubsumption == null) {
            this.destinationSubsumption = true;
        }

        NativeIdSetBI relTypeSet = new ConcurrentBitSet();
        relTypeSet.add(this.relType.getNid());
        if (this.relTypeSubsumption) {
            relTypeSet.or(Ts.get().isKindOfSet(this.relType.getNid(), viewCoordinate));
        }

        NativeIdSetBI destinationSet = new ConcurrentBitSet();
        destinationSet.add(this.destinationSpec.getNid());
        if (this.destinationSubsumption) {
            destinationSet.or(Ts.get().isKindOfSet(this.destinationSpec.getNid(), viewCoordinate));
        }

        NativeIdSetBI restrictionSet = Ts.get().isKindOfSet(relRestrictionSpec.getNid(), viewCoordinate);

        NativeIdSetItrBI iter = destinationSet.getSetBitIterator();
        while (iter.next()) {
            int[] destRelNids = Bdb.getMemoryCache().getDestRelNids(iter.nid());
            for (int i : destRelNids) {
                RelationshipChronicleBI rel = (RelationshipChronicleBI) Ts.get().getComponent(i);
                RelationshipVersionBI relVersion = rel.getVersion(viewCoordinate);
                if (relVersion != null) {
                    if (relTypeSet.contains(relVersion.getTypeNid()) && relVersion.isActive()) {
                        this.addToResultsCache(rel.getConceptNid());
                    }
                }
            }
        }

        getResultsCache().and(restrictionSet);

        return getResultsCache();

    }

    @Override
    public void getQueryMatches(ConceptVersionBI conceptVersion) throws IOException, ContradictionException {
        //Nothing to do here...
    }
}
