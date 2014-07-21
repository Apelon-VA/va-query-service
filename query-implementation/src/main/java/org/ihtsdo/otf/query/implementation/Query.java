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

import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.query.implementation.clauses.ConceptIsKindOf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.ihtsdo.otf.tcc.api.concept.ConceptFetcherBI;
import org.ihtsdo.otf.tcc.api.concept.ProcessUnfetchedConceptDataBI;
import org.ihtsdo.otf.tcc.api.store.Ts;
import org.ihtsdo.otf.tcc.api.concept.ConceptVersionBI;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.api.coordinate.StandardViewCoordinates;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetItrBI;
import org.ihtsdo.otf.query.implementation.clauses.ChangedFromPreviousVersion;
import org.ihtsdo.otf.query.implementation.clauses.ConceptForComponent;
import org.ihtsdo.otf.query.implementation.clauses.ConceptIs;
import org.ihtsdo.otf.query.implementation.clauses.ConceptIsChildOf;
import org.ihtsdo.otf.query.implementation.clauses.ConceptIsDescendentOf;
import org.ihtsdo.otf.query.implementation.clauses.DescriptionActiveLuceneMatch;
import org.ihtsdo.otf.query.implementation.clauses.DescriptionActiveRegexMatch;
import org.ihtsdo.otf.query.implementation.clauses.DescriptionLuceneMatch;
import org.ihtsdo.otf.query.implementation.clauses.DescriptionRegexMatch;
import org.ihtsdo.otf.query.implementation.clauses.FullySpecifiedNameForConcept;
import org.ihtsdo.otf.query.implementation.clauses.PreferredNameForConcept;
import org.ihtsdo.otf.query.implementation.clauses.RefsetContainsConcept;
import org.ihtsdo.otf.query.implementation.clauses.RefsetContainsKindOfConcept;
import org.ihtsdo.otf.query.implementation.clauses.RefsetContainsString;
import org.ihtsdo.otf.query.implementation.clauses.RefsetLuceneMatch;
import org.ihtsdo.otf.query.implementation.clauses.RelRestriction;
import org.ihtsdo.otf.query.implementation.clauses.RelType;
import org.ihtsdo.otf.tcc.api.chronicle.ComponentChronicleBI;
import org.ihtsdo.otf.tcc.api.chronicle.ComponentVersionBI;
import org.ihtsdo.otf.tcc.api.description.DescriptionChronicleBI;
import org.ihtsdo.otf.tcc.api.description.DescriptionVersionBI;
import org.ihtsdo.otf.tcc.ddo.ComponentReference;
import org.ihtsdo.otf.tcc.ddo.concept.ConceptChronicleDdo;
import org.ihtsdo.otf.tcc.ddo.concept.component.description.DescriptionChronicleDdo;
import org.ihtsdo.otf.tcc.ddo.concept.component.description.DescriptionVersionDdo;
import org.ihtsdo.otf.tcc.ddo.fetchpolicy.RefexPolicy;
import org.ihtsdo.otf.tcc.ddo.fetchpolicy.RelationshipPolicy;
import org.ihtsdo.otf.tcc.ddo.fetchpolicy.VersionPolicy;

/**
 * Executes queries within the terminology hierarchy and returns the nids of the
 * components that match the criterion of query.
 *
 * @author kec
 */
public abstract class Query {

    public String currentViewCoordinateKey = "Current view coordinate";
    private HashMap<String, Object> letDeclarations
            = new HashMap<>();

    public HashMap<String, Object> getLetDeclarations() {
        return letDeclarations;
    }

    public void setLetDelclarations(HashMap<String, Object> letMap) {
        this.letDeclarations = letMap;
    }

    public HashMap<String, Object> getVCLetDeclarations() throws IOException {
        HashMap<String, Object> letVCDeclarations
                = new HashMap<>();
        letVCDeclarations.put(currentViewCoordinateKey, StandardViewCoordinates.getSnomedInferredLatestActiveOnly());
        return letVCDeclarations;
    }
    /**
     * Number of Components output in the returnResultSet method.
     */
    int resultSetLimit = 50;

    /**
     * The concepts, stored as nids in a <code>NativeIdSetBI</code>, that are
     * considered in the query.
     */
    private NativeIdSetBI forSet;
    /**
     * The steps required to compute the query clause.
     */
    private EnumSet<ClauseComputeType> computeTypes
            = EnumSet.noneOf(ClauseComputeType.class);
    /**
     * The <code>ViewCoordinate</code> used in the query.
     */
    private ViewCoordinate viewCoordinate;

    /**
     * Retrieves what type of iterations are required to compute the clause.
     *
     * @return an <code>EnumSet</code> of the compute types required
     */
    public EnumSet<ClauseComputeType> getComputePhases() {
        return computeTypes;
    }

    /**
     * No argument constructor, which creates a <code>Query</code> with the
     * Snomed inferred latest as the input <code>ViewCoordinate</code>.
     */
    public Query() {
        this(null);
    }

    /**
     * Constructor for <code>Query</code>. If a <code>ViewCoordinate</code> is
     * not specified, the default is the Snomed inferred latest.
     *
     * @param viewCoordinate
     */
    public Query(ViewCoordinate viewCoordinate) {
        if (viewCoordinate == null) {
            try {
                this.viewCoordinate = StandardViewCoordinates.getSnomedInferredLatestActiveOnly();
            } catch (IOException ex) {
                Logger.getLogger(Query.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else {
            this.viewCoordinate = viewCoordinate;
        }
    }

    /**
     * Determines the set that will be searched in the query.
     *
     * @return the <code>NativeIdSetBI</code> of the set that will be queried
     * @throws IOException
     */
    protected abstract NativeIdSetBI For() throws IOException;

    public abstract void Let() throws IOException;

    /**
     * Retrieves the root clause of the query.
     *
     * @return root <code>Clause</code> in the query
     */
    public abstract Clause Where();

    public void let(String key, Object object) throws IOException {
        letDeclarations.put(key, object);
    }

    /**
     * Constructs the query and computes the set of concepts that match the
     * criterion specified in the clauses.
     *
     * @return the <code>NativeIdSetBI</code> of nids that meet the criterion of
     * the query
     * @throws IOException
     * @throws Exception
     */
    public NativeIdSetBI compute() throws IOException, Exception {
        forSet = For();
        Let();
        Clause rootClause = Where();
        NativeIdSetBI possibleComponents
                = rootClause.computePossibleComponents(forSet);
        if (computeTypes.contains(ClauseComputeType.ITERATION)) {
            NativeIdSetBI conceptsToIterateOver
                    = Ts.get().getConceptNidsForComponentNids(possibleComponents);
            Iterator itr = new Iterator(rootClause, conceptsToIterateOver);
            Ts.get().iterateConceptDataInParallel(itr);
        }
        return rootClause.computeComponents(possibleComponents);
    }

    /**
     *
     * @return the <code>ViewCoordinate</code> in the query
     */
    public ViewCoordinate getViewCoordinate() {
        return viewCoordinate;
    }

    public void setViewCoordinate(ViewCoordinate vc) {
        this.viewCoordinate = vc;
    }

    public static ArrayList<Object> returnDisplayObjects(NativeIdSetBI resultSet, ReturnTypes returnType, ViewCoordinate vc) throws ContradictionException, UnsupportedOperationException, IOException {
        ArrayList<Object> results = new ArrayList<>();

        NativeIdSetItrBI iter = resultSet.getSetBitIterator();
        switch (returnType) {
            case UUIDS:
                while (iter.next()) {
                    ComponentReference cf = new ComponentReference(Ts.get().getSnapshot(vc), iter.nid());
                    if (!cf.componentVersionIsNull()) {
                        results.add(cf.getUuid());
                    }
                }
                break;
            case NIDS:
                while (iter.next()) {
                    results.add(iter.nid());
                }
                break;
            case CONCEPT_VERSION:
                while (iter.next()) {
                    ConceptChronicleDdo cc = new ConceptChronicleDdo(Ts.get().getSnapshot(vc), Ts.get().getConcept(iter.nid()), VersionPolicy.ACTIVE_VERSIONS,
                            RefexPolicy.REFEX_MEMBERS_AND_REFSET_MEMBERS, RelationshipPolicy.DESTINATION_RELATIONSHIPS);
                    results.add(cc);
                }
                break;
            case COMPONENT:
                while (iter.next()) {
                    ComponentReference cf = new ComponentReference(Ts.get().getSnapshot(vc), iter.nid());
                    if (!cf.componentVersionIsNull()) {
                        results.add(cf);
                    }
                }
                break;
            case DESCRIPTION_VERSION_FSN:
                while (iter.next()) {
                    ComponentVersionBI cv = Ts.get().getComponent(iter.nid()).getVersion(vc);
                    if (cv != null) {
                        DescriptionChronicleBI desc = Ts.get().getConceptVersion(vc, iter.nid()).getFullySpecifiedDescription();
                        ConceptChronicleDdo cc = new ConceptChronicleDdo(Ts.get().getSnapshot(vc), Ts.get().getConcept(iter.nid()), VersionPolicy.ACTIVE_VERSIONS,
                                RefexPolicy.REFEX_MEMBERS_AND_REFSET_MEMBERS, RelationshipPolicy.DESTINATION_RELATIONSHIPS);
                        DescriptionChronicleDdo descChronicle = new DescriptionChronicleDdo(Ts.get().getSnapshot(vc), cc, desc);
                        DescriptionVersionBI descVersionBI = desc.getVersion(vc);
                        DescriptionVersionDdo descVersion = new DescriptionVersionDdo(descChronicle, Ts.get().getSnapshot(vc), descVersionBI);
                        results.add(descVersion);
                    }
                }
                break;

            case DESCRIPTION_VERSION_PREFERRED:
                while (iter.next()) {
                    int componentNid = iter.nid();
                    ComponentVersionBI cv = Ts.get().getComponent(componentNid).getVersion(vc);
                    if (cv != null) {
                        if (!(cv instanceof ConceptVersionBI)) {
                            componentNid = Ts.get().getComponent(componentNid).getEnclosingConcept().getConceptNid();
                        }
                        DescriptionChronicleBI desc = Ts.get().getConceptVersion(vc, componentNid).getPreferredDescription();
                        ConceptChronicleDdo cc = new ConceptChronicleDdo(Ts.get().getSnapshot(vc), Ts.get().getConcept(componentNid), VersionPolicy.ACTIVE_VERSIONS,
                                RefexPolicy.REFEX_MEMBERS_AND_REFSET_MEMBERS, RelationshipPolicy.DESTINATION_RELATIONSHIPS);
                        DescriptionChronicleDdo descChronicle = new DescriptionChronicleDdo(Ts.get().getSnapshot(vc), cc, desc);
                        DescriptionVersionBI descVersionBI = desc.getVersion(vc);
                        DescriptionVersionDdo descVersion = new DescriptionVersionDdo(descChronicle, Ts.get().getSnapshot(vc), descVersionBI);
                        results.add(descVersion);
                    }
                }
                break;
            case DESCRIPTION_FOR_COMPONENT:
                while (iter.next()) {
                    ComponentChronicleBI component = Ts.get().getComponent(iter.nid());
                    if (component == null) {
                        System.out.println("No component for nid: " + iter.nid());
                    }
                    if (component != null) {
                        ComponentVersionBI cv = Ts.get().getComponent(iter.nid()).getVersion(vc);
                        if (cv != null) {
                            DescriptionChronicleBI desc = null;
                            ConceptChronicleDdo cc = null;
                            DescriptionVersionBI descVersionBI = null;
                            if (cv instanceof ConceptVersionBI) {
                                desc = Ts.get().getConceptVersion(vc, iter.nid()).getFullySpecifiedDescription();
                                descVersionBI = desc.getVersion(vc);
                                cc = new ConceptChronicleDdo(Ts.get().getSnapshot(vc), Ts.get().getConcept(iter.nid()), VersionPolicy.ACTIVE_VERSIONS,
                                        RefexPolicy.REFEX_MEMBERS_AND_REFSET_MEMBERS, RelationshipPolicy.DESTINATION_RELATIONSHIPS);
                            } else if (cv instanceof DescriptionVersionBI) {
                                desc = (DescriptionChronicleBI) Ts.get().getComponent(iter.nid());
                                descVersionBI = (DescriptionVersionBI) cv;
                                cc = new ConceptChronicleDdo(Ts.get().getSnapshot(vc), Ts.get().getComponent(iter.nid()).getEnclosingConcept(), VersionPolicy.ACTIVE_VERSIONS,
                                        RefexPolicy.REFEX_MEMBERS_AND_REFSET_MEMBERS, RelationshipPolicy.DESTINATION_RELATIONSHIPS);
                            } else {
                                throw new UnsupportedOperationException("This component type is not yet supported");
                            }
                            DescriptionChronicleDdo descChronicle = new DescriptionChronicleDdo(Ts.get().getSnapshot(vc), cc, desc);
                            DescriptionVersionDdo descVersion = new DescriptionVersionDdo(descChronicle, Ts.get().getSnapshot(vc), descVersionBI);
                            results.add(descVersion);
                        }
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException("Return type not supported.");
        }

        return results;
    }

    private class Iterator implements ProcessUnfetchedConceptDataBI {

        NativeIdSetBI conceptsToIterate;
        Clause rootClause;

        public Iterator(Clause rootClause, NativeIdSetBI conceptsToIterate) {
            this.rootClause = rootClause;
            this.conceptsToIterate = conceptsToIterate;
        }

        @Override
        public boolean allowCancel() {
            return true;
        }

        @Override
        public void processUnfetchedConceptData(int cNid, ConceptFetcherBI fetcher) throws Exception {
            if (conceptsToIterate.contains(cNid)) {
                ConceptVersionBI concept = fetcher.fetch(viewCoordinate);
                for (Clause c : rootClause.getChildren()) {
                    c.getQueryMatches(concept);
                }
            }
        }

        @Override
        public NativeIdSetBI getNidSet() throws IOException {
            return conceptsToIterate;
        }

        @Override
        public String getTitle() {
            return "Query Iterator";
        }

        @Override
        public boolean continueWork() {
            return true;
        }
    }

    /**
     * The default method for computing query results, which returns the fully
     * specified description version of the components from the
     * <code>Query</code>.
     *
     * @param q input <code>Query</code>
     * @return The result set of the <code>Query</code> in * * * * * * * * * *
     * an <code>ArrayList</code> of <code>DescriptionVersionDdo</code> objects
     * @throws IOException
     * @throws ContradictionException
     * @throws Exception
     */
    public ArrayList<Object> returnResults() throws IOException, ContradictionException, Exception {
        NativeIdSetBI resultSet = compute();
        return returnDisplayObjects(resultSet, ReturnTypes.DESCRIPTION_VERSION_FSN);
    }

    /**
     * Return the desired Display Objects, which are specified by
     * <code>ReturnTypes</code>.
     *
     * @param resultSet results from the Query
     * @param returnType an <code>EnumSet</code> of <code>ReturnTypes</code>,
     * the desired Display Object types
     * @return an <code>ArrayList</code> of the Display Objects
     * @throws IOException
     * @throws ContradictionException
     */
    public ArrayList<Object> returnDisplayObjects(NativeIdSetBI resultSet, ReturnTypes returnType) throws IOException, ContradictionException {
        return returnDisplayObjects(resultSet, returnType, viewCoordinate);

    }

    public void setResultSetLimit(int limit) {
        this.resultSetLimit = limit;
    }

    /**
     * Creates <code>ConceptIsKindOf</code> clause with default
     * <code>ViewCoordinate</code>.
     *
     * @param conceptSpecKey
     * @return
     */
    protected ConceptIsKindOf ConceptIsKindOf(String conceptSpecKey) {
        return new ConceptIsKindOf(this, conceptSpecKey, this.currentViewCoordinateKey);
    }

    /**
     * Creates <code>ConceptIsKindOf</code> clause with input
     * <code>ViewCoordinate</code>.
     *
     * @param conceptSpecKey
     * @param viewCoordinateKey
     * @return
     */
    protected ConceptIsKindOf ConceptIsKindOf(String conceptSpecKey, String viewCoordinateKey) {
        return new ConceptIsKindOf(this, conceptSpecKey, viewCoordinateKey);
    }

    protected DescriptionRegexMatch DescriptionRegexMatch(String regexKey) {
        return new DescriptionRegexMatch(this, regexKey, this.currentViewCoordinateKey);
    }

    protected DescriptionRegexMatch DescriptionRegexMatch(String regexKey, String viewCoordinateKey) {
        return new DescriptionRegexMatch(this, regexKey, viewCoordinateKey);
    }

    protected DescriptionActiveRegexMatch DescriptionActiveRegexMatch(String regexKey) {
        return new DescriptionActiveRegexMatch(this, regexKey, this.currentViewCoordinateKey);
    }

    protected DescriptionActiveRegexMatch DescriptionActiveRegexMatch(String regexKey, String viewCoordinateKey) {
        return new DescriptionActiveRegexMatch(this, regexKey, viewCoordinateKey);
    }

    /**
     * Creates <code>ConceptForComponent</code> clause with input child clause.
     *
     * @param child
     * @return
     */
    protected ConceptForComponent ConceptForComponent(Clause child) {
        return new ConceptForComponent(this, child);
    }

    protected ConceptIs ConceptIs(String conceptSpecKey) {
        return new ConceptIs(this, conceptSpecKey, this.currentViewCoordinateKey);
    }

    /**
     * Creates <code>ConceptIs</code> clause with input
     * <code>ViewCoordinate</code>.
     *
     * @param conceptSpecKey
     * @param viewCoordinateKey
     * @return
     */
    protected ConceptIs ConceptIs(String conceptSpecKey, String viewCoordinateKey) {
        return new ConceptIs(this, conceptSpecKey, viewCoordinateKey);
    }

    protected ConceptIsDescendentOf ConceptIsDescendentOf(String conceptSpecKey) {
        return new ConceptIsDescendentOf(this, conceptSpecKey, this.currentViewCoordinateKey);
    }

    /**
     * Creates <code>ConceptIsDescendentOf</code> clause with input
     * <code>ViewCoordinate</code>.
     *
     * @param conceptSpecKey
     * @param viewCoordinateKey
     * @return
     */
    protected ConceptIsDescendentOf ConceptIsDescendentOf(String conceptSpecKey, String viewCoordinateKey) {
        return new ConceptIsDescendentOf(this, conceptSpecKey, viewCoordinateKey);
    }

    protected ConceptIsChildOf ConceptIsChildOf(String conceptSpecKey) {
        return new ConceptIsChildOf(this, conceptSpecKey, this.currentViewCoordinateKey);
    }

    /**
     * Creates <code>ConceptIsChildOf</code> clause with input
     * <code>ViewCoordinate</code>.
     *
     * @param conceptSpecKey
     * @param viewCoordinateKey
     * @return
     */
    protected ConceptIsChildOf ConceptIsChildOf(String conceptSpecKey, String viewCoordinateKey) {
        return new ConceptIsChildOf(this, conceptSpecKey, viewCoordinateKey);
    }

    protected DescriptionActiveLuceneMatch DescriptionActiveLuceneMatch(String queryTextKey) {
        return new DescriptionActiveLuceneMatch(this, queryTextKey, this.currentViewCoordinateKey);
    }

    protected DescriptionActiveLuceneMatch DescriptionActiveLuceneMatch(String queryTextKey, String viewCoordinateKey) {
        return new DescriptionActiveLuceneMatch(this, queryTextKey, viewCoordinateKey);
    }

    protected DescriptionLuceneMatch DescriptionLuceneMatch(String queryTextKey) {
        return new DescriptionLuceneMatch(this, queryTextKey, this.currentViewCoordinateKey);
    }

    protected And And(Clause... clauses) {
        return new And(this, clauses);
    }

    protected RelType RelType(String relTypeKey, String conceptSpecKey) {
        return new RelType(this, relTypeKey, conceptSpecKey, this.currentViewCoordinateKey, true);
    }

    /**
     * Creates <code>RelType</code> clause with input
     * <code>ViewCoordinate</code>.
     *
     * @param relTypeKey
     * @param conceptSpecKey
     * @param viewCoordinateKey
     * @return
     */
    protected RelType RelType(String relTypeKey, String conceptSpecKey, String viewCoordinateKey) {
        return new RelType(this, relTypeKey, conceptSpecKey, viewCoordinateKey, true);
    }

    protected RelType RelType(String relTypeKey, String conceptSpecKey, Boolean subsumption) {
        return new RelType(this, relTypeKey, conceptSpecKey, this.currentViewCoordinateKey, subsumption);
    }

    protected RelType RelType(String relTypeKey, String conceptSpecKey, String viewCoordinateKey, Boolean subsumption) {
        return new RelType(this, relTypeKey, conceptSpecKey, viewCoordinateKey, subsumption);
    }

    protected RelRestriction RelRestriction(String restrictionSpecKey, String relTypeKey, String destinationSpecKey) {
        return new RelRestriction(this, restrictionSpecKey, relTypeKey, destinationSpecKey, this.currentViewCoordinateKey, null, null);
    }

    protected RelRestriction RelRestriction(String restrictionSpecKey, String relTypeKey, String destinationSpecKey, String key) {
        if (this.letDeclarations.get(key) instanceof Boolean) {
            return new RelRestriction(this, restrictionSpecKey, relTypeKey, destinationSpecKey, this.currentViewCoordinateKey, key, null);
        } else {
            return new RelRestriction(this, restrictionSpecKey, relTypeKey, destinationSpecKey, key, null, null);
        }
    }

    protected RelRestriction RelRestriction(String restrictionSpecKey, String relTypeKey, String destinatonSpecKey, String key1, String key2) {
        if (this.letDeclarations.get(key2) instanceof Boolean) {
            return new RelRestriction(this, restrictionSpecKey, relTypeKey, destinatonSpecKey, this.currentViewCoordinateKey, key1, key2);
        } else {
            return new RelRestriction(this, restrictionSpecKey, relTypeKey, destinatonSpecKey, key1, key2, null);
        }
    }

    protected RelRestriction RelRestriction(String restrictionSpecKey, String relTypeKey, String destinationSpecKey, String viewCoordinateKey, String relTypeSubsumptionKey, String targetSubsumptionKey) {
        return new RelRestriction(this, restrictionSpecKey, relTypeKey, destinationSpecKey, viewCoordinateKey, relTypeSubsumptionKey, targetSubsumptionKey);
    }

    protected RefsetContainsConcept RefsetContainsConcept(String refsetSpecKey, String conceptSpecKey) {
        return new RefsetContainsConcept(this, refsetSpecKey, conceptSpecKey, this.currentViewCoordinateKey);
    }

    protected RefsetContainsConcept RefsetContainsConcept(String refsetSpecKey, String conceptSpecKey, String viewCoordinateKey) {
        return new RefsetContainsConcept(this, refsetSpecKey, conceptSpecKey, viewCoordinateKey);
    }

    protected RefsetContainsKindOfConcept RefsetContainsKindOfConcept(String refsetSpecKey, String conceptSpecKey) {
        return new RefsetContainsKindOfConcept(this, refsetSpecKey, conceptSpecKey, this.currentViewCoordinateKey);
    }

    protected RefsetContainsKindOfConcept RefsetContainsKindOfConcept(String refsetSpecKey, String conceptSpecKey, String viewCoordinateKey) {
        return new RefsetContainsKindOfConcept(this, refsetSpecKey, conceptSpecKey, viewCoordinateKey);
    }

    protected RefsetContainsString RefsetContainsString(String refsetSpecKey, String stringMatchKey) {
        return new RefsetContainsString(this, refsetSpecKey, stringMatchKey, this.currentViewCoordinateKey);
    }

    protected RefsetContainsString RefsetContainsString(String refsetSpecKey, String stringMatchKey, String viewCoordinateKey) {
        return new RefsetContainsString(this, refsetSpecKey, stringMatchKey, viewCoordinateKey);
    }

    protected RefsetLuceneMatch RefsetLuceneMatch(String queryString) {
        return new RefsetLuceneMatch(this, queryString, this.currentViewCoordinateKey);
    }

    protected PreferredNameForConcept PreferredNameForConcept(Clause clause) {
        return new PreferredNameForConcept(this, clause);
    }

    protected And Intersection(Clause... clauses) {
        return new And(this, clauses);
    }

    protected FullySpecifiedNameForConcept FullySpecifiedNameForConcept(Clause clause) {
        return new FullySpecifiedNameForConcept(this, clause);
    }

    public Not Not(Clause clause) {
        return new Not(this, clause);
    }

    /**
     * Getter for the For set.
     *
     * @return the <code>NativeIdSetBI</code> of the concepts that will be
     * searched in the query
     */
    public NativeIdSetBI getForSet() {
        return forSet;
    }

    protected Or Or(Clause... clauses) {
        return new Or(this, clauses);
    }

    protected Or Union(Clause... clauses) {
        return new Or(this, clauses);
    }

    protected ChangedFromPreviousVersion ChangedFromPreviousVersion(String previousCoordinateKey) {
        return new ChangedFromPreviousVersion(this, previousCoordinateKey);
    }

    protected Xor Xor(Clause... clauses) {
        return new Xor(this, clauses);
    }

    protected AndNot AndNot(Clause... clauses) {
        return new AndNot(this, clauses);
    }
}
