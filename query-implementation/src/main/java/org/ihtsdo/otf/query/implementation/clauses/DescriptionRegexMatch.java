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
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.tcc.api.concept.ConceptVersionBI;
import org.ihtsdo.otf.tcc.api.description.DescriptionChronicleBI;
import org.ihtsdo.otf.tcc.api.description.DescriptionVersionBI;
import org.ihtsdo.otf.tcc.api.nid.ConcurrentBitSet;
import org.ihtsdo.otf.query.implementation.ClauseSemantic;
import org.ihtsdo.otf.query.implementation.WhereClause;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Calculates descriptions that match the specified Java Regular Expression.
 * Very slow when iterating over a large
 * {@link org.ihtsdo.otf.query.implementation.ForCollection} set.
 *
 * @author kec
 */
@XmlRootElement
@XmlAccessorType(value = XmlAccessType.NONE)
public class DescriptionRegexMatch extends LeafClause {

    NativeIdSetBI cache = new ConcurrentBitSet();
    @XmlElement
    String regexKey;
    @XmlElement
    String viewCoordinateKey;

    public DescriptionRegexMatch(Query enclosingQuery, String regexKey, String viewCoordinateKey) {
        super(enclosingQuery);
        this.viewCoordinateKey = viewCoordinateKey;
        this.regexKey = regexKey;
    }
    protected DescriptionRegexMatch() {
    }
    @Override
    public EnumSet<ClauseComputeType> getComputePhases() {
        return ITERATION;
    }

    @Override
    public NativeIdSetBI computePossibleComponents(NativeIdSetBI incomingPossibleComponents) throws IOException {
        this.cache = incomingPossibleComponents;
        return incomingPossibleComponents;
    }

    @Override
    public void getQueryMatches(ConceptVersionBI conceptVersion) throws IOException, ContradictionException {
        String regex = (String) enclosingQuery.getLetDeclarations().get(regexKey);
        for (DescriptionChronicleBI dc : conceptVersion.getDescriptions()) {
            if (cache.contains(dc.getNid())) {
                for (DescriptionVersionBI dv : dc.getVersions()) {
                    if (dv.getText().matches(regex)) {
                        addToResultsCache((dv.getNid()));
                    }
                }
            }
        }
    }

    @Override
    public WhereClause getWhereClause() {
        WhereClause whereClause = new WhereClause();
        whereClause.setSemantic(ClauseSemantic.DESCRIPTION_REGEX_MATCH);
        whereClause.getLetKeys().add(regexKey);
        whereClause.getLetKeys().add(viewCoordinateKey);
        return whereClause;
    }
}
