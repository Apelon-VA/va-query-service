/*
 * Copyright 2014 International Health Terminology Standards Development Organisation.
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
package org.ihtsdo.otf.query.implementation;

import java.io.IOException;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.api.nid.ConcurrentBitSet;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.tcc.api.spec.ValidationException;

import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author dylangrald
 */
@XmlRootElement()
public class AndNot extends ParentClause {

    public AndNot(Query enclosingQuery, Clause... clauses) {
        super(enclosingQuery, clauses);
    }
    /**
     * Default no arg constructor for Jaxb.
     */
    protected AndNot() {
        super();
    }

    @Override
    public NativeIdSetBI computePossibleComponents(NativeIdSetBI incomingPossibleComponents) throws IOException, ValidationException, ContradictionException {
        NativeIdSetBI results = new ConcurrentBitSet(incomingPossibleComponents);
        for (Clause clause : getChildren()) {
            results.andNot(clause.computePossibleComponents(incomingPossibleComponents));
        }
        return results;
    }

    @Override
    public WhereClause getWhereClause() {
        WhereClause whereClause = new WhereClause();
        whereClause.setSemantic(ClauseSemantic.AND_NOT);
        for (Clause clause : getChildren()) {
            whereClause.getChildren().add(clause.getWhereClause());
        }
        return whereClause;
    }

    @Override
    public NativeIdSetBI computeComponents(NativeIdSetBI incomingComponents) throws IOException, ValidationException, ContradictionException {
        NativeIdSetBI results = new ConcurrentBitSet(incomingComponents);
        for (Clause clause : getChildren()) {
            results.andNot(clause.computeComponents(incomingComponents));
        }
        return results;
    }
}
