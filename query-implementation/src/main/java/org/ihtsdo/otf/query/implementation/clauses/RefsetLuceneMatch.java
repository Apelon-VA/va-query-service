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

import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.chronicle.LatestVersion;
import gov.vha.isaac.ochre.api.chronicle.ObjectChronology;
import gov.vha.isaac.ochre.api.chronicle.StampedVersion;
import gov.vha.isaac.ochre.api.component.concept.ConceptVersion;
import java.util.EnumSet;
import java.util.List;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.query.implementation.ClauseComputeType;
import org.ihtsdo.otf.query.implementation.ClauseSemantic;
import org.ihtsdo.otf.query.implementation.LeafClause;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.query.implementation.WhereClause;
import org.ihtsdo.otf.tcc.api.blueprint.ComponentProperty;
import org.ihtsdo.otf.tcc.model.index.service.IndexerBI;
import gov.vha.isaac.ochre.api.index.SearchResult;
import gov.vha.isaac.ochre.collections.NidSet;
import java.util.Optional;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Retrieves the refset matching the input SNOMED id.
 *
 * @author dylangrald
 */
@XmlRootElement
@XmlAccessorType(value = XmlAccessType.NONE)
public class RefsetLuceneMatch extends LeafClause {

    @XmlElement
    String luceneMatchKey;
    @XmlElement
    String viewCoordinateKey;

    public RefsetLuceneMatch(Query enclosingQuery, String luceneMatchKey, String viewCoordinateKey) {
        super(enclosingQuery);
        this.luceneMatchKey = luceneMatchKey;
        this.viewCoordinateKey = viewCoordinateKey;
    }

    protected RefsetLuceneMatch() {
    }

    @Override
    public WhereClause getWhereClause() {
        WhereClause whereClause = new WhereClause();
        whereClause.setSemantic(ClauseSemantic.REFSET_LUCENE_MATCH);
        whereClause.getLetKeys().add(luceneMatchKey);
        return whereClause;
    }

    @Override
    public EnumSet<ClauseComputeType> getComputePhases() {
        return PRE_ITERATION;
    }

    @Override
    public NidSet computePossibleComponents(NidSet incomingPossibleComponents) {
        throw new UnsupportedOperationException();
        //TODO FIX BACK UP
//        ViewCoordinate viewCoordinate = (ViewCoordinate) this.enclosingQuery.getLetDeclarations().get(viewCoordinateKey);
//        String luceneMatch = (String) enclosingQuery.getLetDeclarations().get(luceneMatchKey);
//
//        NidSet nids = new NidSet();
//        List<IndexerBI> lookers = LookupService.get().getAllServices(IndexerBI.class);
//        IndexerBI refexIndexer = null;
//        for (IndexerBI li : lookers) {
//            if (li.getIndexerName().equals("refex")) {
//                refexIndexer = li;
//            }
//        }
//        if (refexIndexer == null) {
//            throw new IllegalStateException("RefexIndexer is null");
//        }
//        List<SearchResult> queryResults = refexIndexer.query(luceneMatch, ComponentProperty.LONG_EXTENSION_1, 1000);
//        queryResults.stream().forEach((s) -> {
//            nids.add(s.nid);
//        });
//        nids.stream().forEach((nid) -> {
//            Optional<? extends ObjectChronology<? extends StampedVersion>> optionalObject
//                    = identifiedObjectService.getIdentifiedObjectChronology(nid);
//            if (optionalObject.isPresent()) {
//                Optional<? extends LatestVersion<? extends StampedVersion>> optionalVersion = 
//                        optionalObject.get().getLatestActiveVersion(viewCoordinate);
//                if (!optionalVersion.isPresent()) {
//                    nids.remove(nid);
//                }
//            } else {
//                nids.remove(nid);
//            }
//        });
//        //Filter the results, based upon the input ViewCoordinate
//        getResultsCache().or(nids);
//        return nids;
    }

    @Override
    public void getQueryMatches(ConceptVersion conceptVersion) {
    }
}
