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
package org.ihtsdo.otf.query.implementation;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.UUID;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import static org.ihtsdo.otf.query.implementation.ClauseSemantic.AND;
import static org.ihtsdo.otf.query.implementation.ClauseSemantic.CHANGED_FROM_PREVIOUS_VERSION;
import static org.ihtsdo.otf.query.implementation.ClauseSemantic.CONCEPT_FOR_COMPONENT;
import static org.ihtsdo.otf.query.implementation.ClauseSemantic.CONCEPT_IS_CHILD_OF;
import static org.ihtsdo.otf.query.implementation.ClauseSemantic.CONCEPT_IS_DESCENDENT_OF;
import static org.ihtsdo.otf.query.implementation.ClauseSemantic.CONCEPT_IS_KIND_OF;
import static org.ihtsdo.otf.query.implementation.ClauseSemantic.DESCRIPTION_ACTIVE_LUCENE_MATCH;
import static org.ihtsdo.otf.query.implementation.ClauseSemantic.DESCRIPTION_ACTIVE_REGEX_MATCH;
import static org.ihtsdo.otf.query.implementation.ClauseSemantic.DESCRIPTION_LUCENE_MATCH;
import static org.ihtsdo.otf.query.implementation.ClauseSemantic.DESCRIPTION_REGEX_MATCH;
import static org.ihtsdo.otf.query.implementation.ClauseSemantic.FULLY_SPECIFIED_NAME_FOR_CONCEPT;
import static org.ihtsdo.otf.query.implementation.ClauseSemantic.NOT;
import static org.ihtsdo.otf.query.implementation.ClauseSemantic.OR;
import static org.ihtsdo.otf.query.implementation.ClauseSemantic.PREFERRED_NAME_FOR_CONCEPT;
import static org.ihtsdo.otf.query.implementation.ClauseSemantic.REFSET_LUCENE_MATCH;
import static org.ihtsdo.otf.query.implementation.ClauseSemantic.REL_TYPE;
import static org.ihtsdo.otf.query.implementation.ClauseSemantic.XOR;
import org.ihtsdo.otf.query.implementation.ForCollection.ForCollectionContents;
import org.ihtsdo.otf.tcc.api.coordinate.SimpleViewCoordinate;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.nid.ConcurrentBitSet;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.tcc.api.spec.ConceptSpec;
import org.ihtsdo.otf.tcc.api.spec.SimpleConceptSpecification;
import org.ihtsdo.otf.tcc.api.spec.ValidationException;
import org.ihtsdo.otf.tcc.api.store.Ts;
import org.ihtsdo.otf.tcc.datastore.BdbTerminologyStore;

/**
 *
 * @author kec
 */
public class QueryFromJaxb extends Query {

    private static ViewCoordinate getViewCoordinate(SimpleViewCoordinate obj) throws ValidationException {
        return new ViewCoordinate((SimpleViewCoordinate) obj);
    }
    private Clause rootClause;
    /**
     * False if a ConceptSpec is declared in LetMap that is null.
     */
    public Boolean nullSpec = false;

    public Clause getRootClause() {
        return rootClause;
    }
    private NativeIdSetBI forCollection;

    public NativeIdSetBI getForCollection() {
        return forCollection;
    }

    public QueryFromJaxb(String viewCoordinateXml, String forXml,
            String letXml, String whereXml) throws JAXBException, IOException {
        super(null);
        if (viewCoordinateXml != null && !viewCoordinateXml.equals("null") && !viewCoordinateXml.equals("")) {
            try {
                setViewCoordinate(getViewCoordinate((SimpleViewCoordinate) JaxbForQuery.get().createUnmarshaller()
                        .unmarshal(new StringReader(viewCoordinateXml))));
            } catch (JAXBException e) {
                this.setViewCoordinate(null);
            }

        }
        Unmarshaller unmarshaller = JaxbForQuery.get().createUnmarshaller();
        BdbTerminologyStore bdb = new BdbTerminologyStore();

        LetMap letMap = null;

        try {
            letMap = (LetMap) unmarshaller.unmarshal(new StringReader(letXml));

        } catch (JAXBException | NullPointerException e) {
            this.setLetDelclarations(null);
        }

        Map<String, Object> convertedMap = null;

        try {
            convertedMap = new HashMap<>(letMap.getMap().size());
            for (Entry entry : letMap.getMap().entrySet()) {
                if (entry.getValue() instanceof SimpleConceptSpecification) {
                    ConceptSpec newValue = new ConceptSpec((SimpleConceptSpecification) entry.getValue());
                    convertedMap.put((String) entry.getKey(), newValue);
                } else {
                    convertedMap.put((String) entry.getKey(), entry.getValue());
                }
            }
            getLetDeclarations().putAll(convertedMap);
        } catch (NullPointerException e) {
            this.setLetDelclarations(null);
        }

        if (forXml == null || forXml.equals("null") || forXml.equals("")) {
            this.forCollection = Ts.get().getAllConceptNids();
        } else if (convertedMap != null) {
            if (convertedMap.containsKey("Custom FOR set")) {
                String UUIDset = (String) convertedMap.get("Custom FOR set");
                ConcurrentBitSet cbs = new ConcurrentBitSet();
                StringTokenizer tok = new StringTokenizer(UUIDset, ",");
                while (tok.hasMoreTokens()) {
                    String next = tok.nextToken();
                    if (next.matches("[0-9a-z-]*")) {
                        UUID nextUUID = UUID.fromString(next);
                        cbs.add(Ts.get().getComponent(nextUUID).getNid());
                    }
                }
                this.forCollection = cbs;
            } else {
                try {
                    ForCollection _for = (ForCollection) unmarshaller.unmarshal(new StringReader(forXml));
                    if (_for.forCollection.equals(ForCollectionContents.CUSTOM)) {
                        this.forCollection = new ConcurrentBitSet();
                        for (UUID i : _for.getCustomCollection()) {
                            forCollection.add(Ts.get().getConcept(i).getConceptNid());
                        }
                    } else {
                        this.forCollection = _for.getCollection();
                    }
                } catch (JAXBException e) {
                    this.forCollection = null;
                }
            }
        }

        try {
            Object obj = unmarshaller.unmarshal(new StringReader(whereXml));
            if (obj instanceof Where) {
                rootClause = getWhereClause(this, ((Where) obj).getRootClause());
            } else {
                rootClause = getWhereClause(this, (WhereClause) obj);
            }
        } catch (JAXBException e) {
            this.rootClause = null;
        } catch (NullPointerException n) {
            this.nullSpec = true;
        }
    }

    @Override
    protected NativeIdSetBI For() throws IOException {
        return this.forCollection;
    }

    @Override
    public void Let() throws IOException {
        // lets are set in the constructor. 
    }

    @Override
    public Clause Where() {
        return rootClause;
    }

    public static Clause getWhereClause(Query q, WhereClause clause) throws IOException {
        Clause[] childClauses = new Clause[clause.children.size()];
        for (int i = 0; i < childClauses.length; i++) {
            WhereClause childClause = clause.children.get(i);
            childClauses[i] = getWhereClause(q, childClause);
        }
        switch (clause.semantic) {
            case AND:
                assert clause.letKeys.isEmpty() : "Let keys should be empty: " + clause.letKeys;
                return q.And(childClauses);
            case NOT:
                assert clause.letKeys.isEmpty() : "Let keys should be empty: " + clause.letKeys;
                assert childClauses.length == 1;
                return q.Not(childClauses[0]);
            case OR:
                assert clause.letKeys.isEmpty() : "Let keys should be empty: " + clause.letKeys;
                return q.Or(childClauses);
            case XOR:
                assert clause.letKeys.isEmpty() : "Let keys should be empty: " + clause.letKeys;
                return q.Xor(childClauses);
            case CONCEPT_FOR_COMPONENT:
                assert clause.letKeys.isEmpty() : "Let keys should be empty: " + clause.letKeys;
                assert childClauses.length == 1;
                return q.ConceptForComponent(childClauses[0]);
            case CONCEPT_IS:
                assert clause.letKeys.size() == 1 || clause.letKeys.size() == 2 : "Let keys should have one or two values: " + clause.letKeys;
                assert childClauses.length == 0 : childClauses;
                if (clause.letKeys.size() == 1) {
                    return q.ConceptIs(clause.letKeys.get(0));
                } else {
                    return q.ConceptIs(clause.letKeys.get(0), clause.letKeys.get(1));
                }
            case CONCEPT_IS_CHILD_OF:
                assert childClauses.length == 0 : childClauses;
                assert clause.letKeys.size() == 1 || clause.letKeys.size() == 2 : "Let keys should have one or two values: " + clause.letKeys;
                if (clause.letKeys.size() == 1) {
                    return q.ConceptIsChildOf(clause.letKeys.get(0));
                } else {
                    return q.ConceptIsChildOf(clause.letKeys.get(0), clause.letKeys.get(1));
                }
            case CONCEPT_IS_DESCENDENT_OF:
                assert childClauses.length == 0 : childClauses;
                assert clause.letKeys.size() == 1 || clause.letKeys.size() == 2 : "Let keys should have one or two values: " + clause.letKeys;
                if (clause.letKeys.size() == 1) {
                    return q.ConceptIsDescendentOf(clause.letKeys.get(0));
                } else {
                    return q.ConceptIsDescendentOf(clause.letKeys.get(0), clause.letKeys.get(1));
                }
            case CONCEPT_IS_KIND_OF:
                assert childClauses.length == 0 : childClauses;
                assert clause.letKeys.size() == 1 || clause.letKeys.size() == 2 : "Let keys should have one or two values: " + clause.letKeys;
                if (clause.letKeys.size() == 1) {
                    return q.ConceptIsKindOf(clause.letKeys.get(0));
                } else {
                    return q.ConceptIsKindOf(clause.letKeys.get(0), clause.letKeys.get(1));
                }
            case CHANGED_FROM_PREVIOUS_VERSION:
                assert childClauses.length == 0 : childClauses;
                assert clause.letKeys.size() == 1 : "Let keys should have one and only one value: " + clause.letKeys;
                return q.ChangedFromPreviousVersion(clause.letKeys.get(0));
            case DESCRIPTION_LUCENE_MATCH:
                assert childClauses.length == 0 : childClauses;
                assert clause.letKeys.size() == 1 : "Let keys should have one and only one value: " + clause.letKeys;
                return q.DescriptionLuceneMatch(clause.letKeys.get(0));
            case DESCRIPTION_ACTIVE_LUCENE_MATCH:
                assert childClauses.length == 0 : childClauses;
                assert clause.letKeys.size() == 2 : "Let keys should have two values: " + clause.letKeys;
                return q.DescriptionActiveLuceneMatch(clause.letKeys.get(0), clause.letKeys.get(1));
            case DESCRIPTION_ACTIVE_REGEX_MATCH:
                assert childClauses.length == 0 : childClauses;
                assert clause.letKeys.size() == 1 || clause.letKeys.size() == 2 : "Let keys should have one or two values: " + clause.letKeys;
                if (clause.letKeys.size() == 1) {
                    return q.DescriptionActiveRegexMatch(clause.letKeys.get(0));
                } else {
                    return q.DescriptionActiveRegexMatch(clause.letKeys.get(0), clause.letKeys.get(1));
                }
            case DESCRIPTION_REGEX_MATCH:
                assert childClauses.length == 0 : childClauses;
                assert clause.letKeys.size() == 1 || clause.letKeys.size() == 2 : "Let keys should have one or two values: " + clause.letKeys;
                if (clause.letKeys.size() == 1) {
                    return q.DescriptionRegexMatch(clause.letKeys.get(0));
                } else {
                    return q.DescriptionRegexMatch(clause.letKeys.get(0), clause.letKeys.get(1));
                }
            case FULLY_SPECIFIED_NAME_FOR_CONCEPT:
                assert clause.letKeys.isEmpty() : "Let keys should be empty: " + clause.letKeys;
                assert childClauses.length == 1;
                return q.FullySpecifiedNameForConcept(childClauses[0]);
            case PREFERRED_NAME_FOR_CONCEPT:
                assert clause.letKeys.isEmpty() : "Let keys should be empty: " + clause.letKeys;
                assert childClauses.length == 1;
                return q.PreferredNameForConcept(childClauses[0]);
            case REFSET_LUCENE_MATCH:
                assert childClauses.length == 0 : childClauses;
                assert clause.letKeys.size() == 1 : "Let keys should have one and only one value " + clause.letKeys;
                return q.RefsetLuceneMatch(clause.letKeys.get(0));
            case REFSET_CONTAINS_CONCEPT:
                assert childClauses.length == 0 : childClauses;
                assert clause.letKeys.size() == 2 || clause.letKeys.size() == 3 : "Let keys should have two or three values: " + clause.letKeys;
                if (clause.letKeys.size() == 1) {
                    return q.RefsetContainsConcept(clause.letKeys.get(0), clause.letKeys.get(1));
                } else {
                    return q.RefsetContainsConcept(clause.letKeys.get(0), clause.letKeys.get(1), clause.letKeys.get(2));
                }
            case REFSET_CONTAINS_KIND_OF_CONCEPT:
                assert childClauses.length == 0 : childClauses;
                assert clause.letKeys.size() == 2 || clause.letKeys.size() == 3 : "Let keys should have two or three values: " + clause.letKeys;
                if (clause.letKeys.size() == 1) {
                    return q.RefsetContainsKindOfConcept(clause.letKeys.get(0), clause.letKeys.get(1));
                } else {
                    return q.RefsetContainsKindOfConcept(clause.letKeys.get(0), clause.letKeys.get(1), clause.letKeys.get(2));
                }
            case REFSET_CONTAINS_STRING:
                assert childClauses.length == 0 : childClauses;
                assert clause.letKeys.size() == 2 || clause.letKeys.size() == 3 : "Let keys should have two or three values: " + clause.letKeys;
                if (clause.letKeys.size() == 1) {
                    return q.RefsetContainsString(clause.letKeys.get(0), clause.letKeys.get(1));
                } else {
                    return q.RefsetContainsString(clause.letKeys.get(0), clause.letKeys.get(1), clause.letKeys.get(2));
                }
            case REL_RESTRICTION:
                assert childClauses.length == 0 : childClauses;
                assert clause.letKeys.size() == 3 || clause.letKeys.size() == 4 || clause.letKeys.size() == 5 || clause.letKeys.size() == 6 : "Let keys hould have three, four, five, or six values: " + clause.letKeys;
                if (clause.letKeys.size() == 3) {
                    return q.RelRestriction(clause.letKeys.get(0), clause.letKeys.get(1), clause.letKeys.get(2));
                } else if (clause.letKeys.size() == 4) {
                    return q.RelRestriction(clause.letKeys.get(0), clause.letKeys.get(1), clause.letKeys.get(2), clause.letKeys.get(3));
                } else if (clause.letKeys.size() == 5) {
                    return q.RelRestriction(clause.letKeys.get(0), clause.letKeys.get(1), clause.letKeys.get(2), clause.letKeys.get(3), clause.letKeys.get(4));
                } else {
                    return q.RelRestriction(clause.letKeys.get(0), clause.letKeys.get(1), clause.letKeys.get(2), clause.letKeys.get(3), clause.letKeys.get(4), clause.letKeys.get(5));
                }
            case REL_TYPE:
                assert childClauses.length == 0 : childClauses;
                assert (clause.letKeys.size() == 2) || (clause.letKeys.size() == 3) : "Let keys should have two or three values: " + clause.letKeys;
                if (clause.letKeys.size() == 2) {
                    return q.RelType(clause.letKeys.get(0), clause.letKeys.get(1));
                } else {
                    return q.RelType(clause.letKeys.get(0), clause.letKeys.get(1), clause.letKeys.get(2));
                }
            default:
                throw new UnsupportedOperationException("Can't handle: " + clause.semantic);
        }
    }
}
