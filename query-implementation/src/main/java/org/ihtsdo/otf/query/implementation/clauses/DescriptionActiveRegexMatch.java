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
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.api.concept.ConceptVersionBI;
import org.ihtsdo.otf.query.implementation.ClauseSemantic;
import org.ihtsdo.otf.query.implementation.WhereClause;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.api.description.DescriptionChronicleBI;
import org.ihtsdo.otf.tcc.api.description.DescriptionVersionBI;

/**
 * Calculates the active descriptions that match the specified Java Regular
 * Expression.
 *
 * @author dylangrald
 */
public class DescriptionActiveRegexMatch extends DescriptionRegexMatch {

    public DescriptionActiveRegexMatch(Query enclosingQuery, String regexKey, String viewCoordinateKey) {
        super(enclosingQuery, regexKey, viewCoordinateKey);
    }

    @Override
    public void getQueryMatches(ConceptVersionBI conceptVersion) throws IOException, ContradictionException {
        for (DescriptionChronicleBI dc : conceptVersion.getDescriptionsActive()) {
            for (DescriptionVersionBI dv : dc.getVersions()) {
                if (dv.getText().matches(regex) && dv.getStatus().compareTo(Status.ACTIVE) == 0) {
                    addToResultsCache((dv.getNid()));
                }
            }
        }
    }

    @Override
    public WhereClause getWhereClause() {
        WhereClause whereClause = new WhereClause();
        whereClause.setSemantic(ClauseSemantic.DESCRIPTION_ACTIVE_REGEX_MATCH);
        whereClause.getLetKeys().add(regexKey);
        whereClause.getLetKeys().add(viewCoordinateKey);
        return whereClause;
    }
}
