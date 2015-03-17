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
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.tcc.api.spec.ConceptSpec;
import org.ihtsdo.otf.tcc.api.spec.ValidationException;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * An identity function that obtains the concept from the input
 * <code>ConceptSpec</code>.
 *
 * @author dylangrald
 */
@XmlRootElement
@XmlAccessorType(value = XmlAccessType.NONE)
public class ConceptIs extends LeafClause {

    @XmlElement
    String conceptSpecString;
    @XmlElement
    String viewCoordinateKey;

    public ConceptIs(Query enclosingQuery, String conceptSpec, String viewCoordinateKey) {
        super(enclosingQuery);
        this.conceptSpecString = conceptSpec;
        this.viewCoordinateKey = viewCoordinateKey;

    }
    protected ConceptIs() {
    }
    @Override
    public WhereClause getWhereClause() {
        WhereClause whereClause = new WhereClause();
        whereClause.setSemantic(ClauseSemantic.CONCEPT_IS);
        whereClause.getLetKeys().add(conceptSpecString);
        whereClause.getLetKeys().add(viewCoordinateKey);
        return whereClause;
    }

    @Override
    public EnumSet<ClauseComputeType> getComputePhases() {
        return PRE_ITERATION;
    }

    @Override
    public NativeIdSetBI computePossibleComponents(NativeIdSetBI incomingPossibleComponents) throws IOException, ValidationException, ContradictionException {
        getResultsCache().add(((ConceptSpec) enclosingQuery.getLetDeclarations().get(conceptSpecString)).getNid());
        return getResultsCache();
    }

    @Override
    public void getQueryMatches(ConceptVersionBI conceptVersion) throws IOException, ContradictionException {
        //Nothing to do here...
    }
}
