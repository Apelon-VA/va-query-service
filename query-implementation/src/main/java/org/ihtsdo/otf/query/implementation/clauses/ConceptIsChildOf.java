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
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.api.store.Ts;
import org.ihtsdo.otf.tcc.api.concept.ConceptVersionBI;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.query.implementation.Where;
import org.ihtsdo.otf.query.implementation.WhereClause;
import org.ihtsdo.otf.tcc.api.spec.ConceptSpec;
import org.ihtsdo.otf.tcc.api.spec.ValidationException;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Computes the set of concepts that are a child of the input concept. The set
 * of children of a given concept is the set of all concepts that lie one step
 * below the input concept in the terminology hierarchy. This
 * <code>Clause</code> has an optional parameter for a previous
 * <code>ViewCoordinate</code>, which allows for queries of previous versions.
 *
 * @author dylangrald
 */
@XmlRootElement
@XmlAccessorType(value = XmlAccessType.NONE)
public class ConceptIsChildOf extends LeafClause {


    @XmlElement
    String kindOfSpecKey;
    @XmlElement
    String viewCoordinateKey;

    public ConceptIsChildOf(Query enclosingQuery, String kindOfSpecKey, String viewCoordinateKey) {
        super(enclosingQuery);
        this.kindOfSpecKey = kindOfSpecKey;
        this.viewCoordinateKey = viewCoordinateKey;
    }
    protected ConceptIsChildOf() {
    }

    @Override
    public NativeIdSetBI computePossibleComponents(NativeIdSetBI incomingPossibleComponents)
            throws ValidationException, IOException, ContradictionException {
        ViewCoordinate viewCoordinate = (ViewCoordinate) this.enclosingQuery.getLetDeclarations().get(viewCoordinateKey);

        ConceptSpec childOfSpec = (ConceptSpec) enclosingQuery.getLetDeclarations().get(kindOfSpecKey);
        int parentNid = childOfSpec.getNid(viewCoordinate);
        getResultsCache().or(Ts.get().isChildOfSet(parentNid, viewCoordinate));
        return getResultsCache();
    }

    @Override
    public EnumSet<ClauseComputeType> getComputePhases() {
        return PRE_AND_POST_ITERATION;
    }

    @Override
    public void getQueryMatches(ConceptVersionBI conceptVersion) {
        // Nothing to do...
    }

    @Override
    public WhereClause getWhereClause() {
        WhereClause whereClause = new WhereClause();
        whereClause.setSemantic(ClauseSemantic.CONCEPT_IS_CHILD_OF);
        whereClause.getLetKeys().add(kindOfSpecKey);
        whereClause.getLetKeys().add(viewCoordinateKey);
        return whereClause;
    }
}
