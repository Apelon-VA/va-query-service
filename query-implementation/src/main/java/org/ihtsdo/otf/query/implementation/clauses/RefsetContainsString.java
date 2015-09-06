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
import gov.vha.isaac.ochre.api.component.concept.ConceptVersion;
import gov.vha.isaac.ochre.api.coordinate.TaxonomyCoordinate;
import gov.vha.isaac.ochre.collections.NidSet;
import java.util.EnumSet;
import org.ihtsdo.otf.query.implementation.ClauseComputeType;
import org.ihtsdo.otf.query.implementation.ClauseSemantic;
import org.ihtsdo.otf.query.implementation.LeafClause;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.query.implementation.WhereClause;
import org.ihtsdo.otf.tcc.api.concept.ConceptVersionBI;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import static org.ihtsdo.otf.tcc.api.refex.RefexType.CID_CID_CID_STRING;
import static org.ihtsdo.otf.tcc.api.refex.RefexType.CID_CID_STR;
import static org.ihtsdo.otf.tcc.api.refex.RefexType.CID_STR;
import static org.ihtsdo.otf.tcc.api.refex.RefexType.STR;
import org.ihtsdo.otf.tcc.api.refex.RefexVersionBI;
import org.ihtsdo.otf.tcc.api.refex.type_string.RefexStringVersionBI;
import org.ihtsdo.otf.tcc.api.spec.ConceptSpec;
import org.ihtsdo.otf.tcc.api.store.Ts;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * .
 *
 * @author dylangrald
 */
@XmlRootElement
@XmlAccessorType(value = XmlAccessType.NONE)
public class RefsetContainsString extends LeafClause {


    @XmlElement
    String queryText;
    @XmlElement
    String viewCoordinateKey;

    NativeIdSetBI cache;

    @XmlElement
    String refsetSpecKey;

    public RefsetContainsString(Query enclosingQuery, String refsetSpecKey, String queryText, String viewCoordinateKey) {
        super(enclosingQuery);
        this.refsetSpecKey = refsetSpecKey;
        this.queryText = queryText;
        this.viewCoordinateKey = viewCoordinateKey;

    }
    protected RefsetContainsString() {
    }
    @Override
    public EnumSet<ClauseComputeType> getComputePhases() {
        return PRE_ITERATION;
    }

    @Override
    public NidSet computePossibleComponents(NidSet incomingPossibleComponents) {

        throw new UnsupportedOperationException();
        //TODO FIX BACK UP
//        TaxonomyCoordinate taxonomyCoordinate = (TaxonomyCoordinate) this.enclosingQuery.getLetDeclarations().get(viewCoordinateKey);
//        ConceptSpec refsetSpec = (ConceptSpec) this.enclosingQuery.getLetDeclarations().get(refsetSpecKey);
//
//        int refsetNid = refsetSpec.getNid();
//        ConceptVersionBI conceptVersion = Ts.get().getConceptVersion(viewCoordinate, refsetNid);
//
//        for (RefexVersionBI<?> rm : conceptVersion.getCurrentRefsetMembers(viewCoordinate)) {
//            switch (rm.getRefexType()) {
//                case CID_STR:
//                case CID_CID_CID_STRING:
//                case CID_CID_STR:
//                case STR:
//                    RefexStringVersionBI rsv = (RefexStringVersionBI) rm;
//                    if (rsv.getString1().toLowerCase().contains(queryText.toLowerCase())) {
//                        getResultsCache().add(refsetNid);
//                    }
//                default:
//                //do nothing
//
//            }
//        }
//
//        return getResultsCache();
    }

    @Override
    public void getQueryMatches(ConceptVersion conceptVersion) {

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
