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

import gov.vha.isaac.ochre.api.chronicle.ObjectChronology;
import gov.vha.isaac.ochre.api.chronicle.StampedVersion;
import gov.vha.isaac.ochre.collections.NidSet;
import java.util.EnumSet;
import java.util.Optional;
import org.ihtsdo.otf.query.implementation.ClauseComputeType;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.query.implementation.ClauseSemantic;
import org.ihtsdo.otf.query.implementation.WhereClause;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 *
 * @author dylangrald
 */
@XmlRootElement
@XmlAccessorType(value = XmlAccessType.NONE)
public class DescriptionActiveLuceneMatch extends DescriptionLuceneMatch {

    public DescriptionActiveLuceneMatch(Query enclosingQuery, String luceneMatchKey, String viewCoordinateKey) {
        super(enclosingQuery, luceneMatchKey, viewCoordinateKey);
    }
    protected DescriptionActiveLuceneMatch() {
    }
    @Override
    public EnumSet<ClauseComputeType> getComputePhases() {
        return PRE_AND_POST_ITERATION;
    }

    @Override
    public final NidSet computeComponents(NidSet incomingComponents) {
        ViewCoordinate viewCoordinate = (ViewCoordinate) this.enclosingQuery.getLetDeclarations().get(viewCoordinateKey);
        getResultsCache().and(incomingComponents);
        
        incomingComponents.stream().forEach((nid) -> {
            Optional<? extends ObjectChronology<? extends StampedVersion>> chronology = 
                    identifiedObjectService.getIdentifiedObjectChronology(nid);
            if (chronology.isPresent()) {
                if (!chronology.get().isLatestVersionActive(viewCoordinate)) {
                    getResultsCache().remove(nid);
                }
            } else {
                getResultsCache().remove(nid);
            }
        });
        return getResultsCache();
    }

    @Override
    public WhereClause getWhereClause() {
        WhereClause whereClause = new WhereClause();
        whereClause.setSemantic(ClauseSemantic.DESCRIPTION_ACTIVE_LUCENE_MATCH);
        whereClause.getLetKeys().add(luceneMatchKey);
        whereClause.getLetKeys().add(viewCoordinateKey);
        return whereClause;
    }
}
