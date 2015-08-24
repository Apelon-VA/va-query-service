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

import gov.vha.isaac.ochre.api.Get;
import gov.vha.isaac.ochre.api.component.concept.ConceptVersion;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import gov.vha.isaac.ochre.collections.NidSet;
import java.io.IOException;
import java.util.EnumSet;
import org.ihtsdo.otf.query.implementation.ClauseComputeType;
import org.ihtsdo.otf.query.implementation.LeafClause;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.tcc.api.store.Ts;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.query.implementation.ClauseSemantic;
import org.ihtsdo.otf.query.implementation.WhereClause;
import org.ihtsdo.otf.tcc.api.spec.ConceptSpec;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

/**
 * Calculates the set of concepts that are a kind of the specified concept. The
 * calculated set is the union of the input concept and all concepts that lie
 * lie beneath the input concept in the terminology hierarchy.
 *
 * @author kec
 */
@XmlAccessorType(value = XmlAccessType.NONE)
public class ConceptIsKindOf extends LeafClause {

    @XmlElement
    String kindOfSpecKey;
    @XmlElement
    String viewCoordinateKey;

    public ConceptIsKindOf(Query enclosingQuery, String kindOfSpecKey, String viewCoordinateKey) {
        super(enclosingQuery);
        this.kindOfSpecKey = kindOfSpecKey;
        this.viewCoordinateKey = viewCoordinateKey;
    }

    protected ConceptIsKindOf() {
        super();
    }

    @Override
    public NidSet computePossibleComponents(NidSet incomingPossibleComponents) {
        try {
            ViewCoordinate viewCoordinate = (ViewCoordinate) this.enclosingQuery.getLetDeclarations().get(viewCoordinateKey);
            ConceptSpec kindOfSpec = (ConceptSpec) enclosingQuery.getLetDeclarations().get(kindOfSpecKey);
            int parentNid = kindOfSpec.getNid(viewCoordinate);
            ConceptSequenceSet kindOfSequenceSet = Get.taxonomyService().getKindOfSequenceSet(parentNid, viewCoordinate.getTaxonomyCoordinate());
            getResultsCache().or(NidSet.of(kindOfSequenceSet));
            return getResultsCache();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public EnumSet<ClauseComputeType> getComputePhases() {
        return PRE_ITERATION;
    }

    @Override
    public void getQueryMatches(ConceptVersion conceptVersion) {
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
