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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.queryparser.classic.ParseException;
import org.ihtsdo.otf.tcc.api.concept.ConceptVersionBI;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.query.implementation.ClauseComputeType;
import org.ihtsdo.otf.query.implementation.ClauseSemantic;
import org.ihtsdo.otf.query.implementation.LeafClause;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.query.implementation.WhereClause;
import org.ihtsdo.otf.tcc.api.blueprint.ComponentProperty;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.api.nid.ConcurrentBitSet;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetItrBI;
import org.ihtsdo.otf.tcc.api.store.Ts;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.ihtsdo.otf.tcc.model.index.service.IndexerBI;
import org.ihtsdo.otf.tcc.model.index.service.SearchResult;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Returns descriptions matching the input string using Lucene.
 *
 * @author kec
 */
@XmlRootElement
@XmlAccessorType(value = XmlAccessType.NONE)
public class DescriptionLuceneMatch extends LeafClause {

    @XmlElement
    String luceneMatchKey;
    @XmlElement
    String viewCoordinateKey;

    public DescriptionLuceneMatch(Query enclosingQuery, String luceneMatchKey, String viewCoordinateKey) {
        super(enclosingQuery);
        this.luceneMatchKey = luceneMatchKey;
        this.viewCoordinateKey = viewCoordinateKey;
    }
    protected DescriptionLuceneMatch() {
    }
    @Override
    public EnumSet<ClauseComputeType> getComputePhases() {
        return PRE_ITERATION;
    }

    @Override
    public final NativeIdSetBI computePossibleComponents(NativeIdSetBI incomingPossibleComponents) throws IOException {
        String luceneMatch = (String) enclosingQuery.getLetDeclarations().get(luceneMatchKey);

        ViewCoordinate viewCoordinate = (ViewCoordinate) this.enclosingQuery.getLetDeclarations().get(viewCoordinateKey);


        NativeIdSetBI nids = new ConcurrentBitSet();
        try {
            List<IndexerBI> lookers = Hk2Looker.get().getAllServices(IndexerBI.class);
            IndexerBI descriptionIndexer = null;
            for (IndexerBI li : lookers) {
                if (li.getIndexerName().equals("descriptions")) {
                    descriptionIndexer = li;
                }
            }
            List<SearchResult> queryResults = descriptionIndexer.query(luceneMatch, ComponentProperty.DESCRIPTION_TEXT, 1000);
            for (SearchResult s : queryResults) {
                nids.add(s.nid);
            }
        } catch (ParseException ex) {
            Logger.getLogger(DescriptionLuceneMatch.class.getName()).log(Level.SEVERE, null, ex);
        }
        //Filter the results, based upon the input ViewCoordinate
        NativeIdSetItrBI iter = nids.getSetBitIterator();
        while (iter.next()) {
            try {
                if (Ts.get().getComponentVersion(viewCoordinate, iter.nid()) == null) {
                    nids.remove(iter.nid());
                }
            } catch (ContradictionException ex) {
                Logger.getLogger(DescriptionLuceneMatch.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        getResultsCache().or(nids);
        return nids;

    }

    @Override
    public void getQueryMatches(ConceptVersionBI conceptVersion) {
        getResultsCache();
    }

    @Override
    public WhereClause getWhereClause() {
        WhereClause whereClause = new WhereClause();
        whereClause.setSemantic(ClauseSemantic.DESCRIPTION_LUCENE_MATCH);
        whereClause.getLetKeys().add(luceneMatchKey);
        return whereClause;
    }
}
