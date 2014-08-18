package org.ihtsdo.otf.query.integration.tests;

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
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.ihtsdo.otf.query.implementation.versioning.StandardViewCoordinates;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.query.implementation.Clause;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.query.implementation.QueryExample;
import org.ihtsdo.otf.query.implementation.ReturnTypes;
import org.ihtsdo.otf.query.integration.tests.rest.TermstoreChanges;
import org.ihtsdo.otf.query.rest.server.AlternativeIdResource;
import org.ihtsdo.otf.tcc.api.blueprint.InvalidCAB;
import org.ihtsdo.otf.tcc.api.chronicle.ComponentChronicleBI;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.api.concept.ConceptVersionBI;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.description.DescriptionChronicleBI;
import org.ihtsdo.otf.tcc.api.description.DescriptionVersionBI;
import org.ihtsdo.otf.tcc.api.nid.ConcurrentBitSet;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetItrBI;
import org.ihtsdo.otf.tcc.api.relationship.RelationshipVersionBI;
import org.ihtsdo.otf.tcc.api.store.Ts;
import org.ihtsdo.otf.tcc.ddo.concept.component.description.DescriptionVersionDdo;
import org.ihtsdo.otf.tcc.junit.BdbTestRunner;
import org.ihtsdo.otf.tcc.junit.BdbTestRunnerConfig;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Class that handles integration tests for
 * {@link org.ihtsdo.otf.query.implementation.Clause} implementations.
 *
 * @author kec
 */
@RunWith(BdbTestRunner.class)
@BdbTestRunnerConfig()
public class QueryTest extends JerseyTest {

    private static final String DIR = System.getProperty("user.dir");
    private static final JSONToReport REPORTS = new JSONToReport(DIR + "/target/test-resources/OTFReports.json");
    private static final Logger LOGGER = Logger.getLogger(QueryTest.class.getName());
    private static ViewCoordinate VC_LATEST_ACTIVE_AND_INACTIVE;
    private static ViewCoordinate VC_LATEST_ACTIVE_ONLY;

    public QueryTest() {
    }

    @Override
    protected Application configure() {
        return new ResourceConfig(AlternativeIdResource.class);
    }

    @BeforeClass
    public static void setUpClass() {
        REPORTS.parseFile();
        try {
            VC_LATEST_ACTIVE_AND_INACTIVE = StandardViewCoordinates.getSnomedInferredLatestActiveAndInactive();
            VC_LATEST_ACTIVE_ONLY = StandardViewCoordinates.getSnomedInferredLatestActiveOnly();
        } catch (IOException ex) {
            Logger.getLogger(QueryTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @AfterClass
    public static void tearDownClass() {
    }

//    @Before
//    public void setUp() throws ValidationException, IOException {
//    }
//    
//    @After
//    public void tearDown() {
//    }
    @Test
    public void testSimpleQuery() throws IOException, Exception {
        LOGGER.log(Level.INFO, "Simple query: ");
        Query q = new Query() {
            @Override
            protected NativeIdSetBI For() throws IOException {
                return null;
            }

            @Override
            public void Let() throws IOException {
                let("motion", Snomed.MOTION);
            }

            @Override
            public Clause Where() {
                return ConceptIs("motion");
            }
        };
        assertEquals(REPORTS.getQueryCount("ConceptIs(Motion)"), q.returnResults().size());
    }

    @Test
    public void testRegexQuery() throws IOException, Exception {
        DescriptionRegexMatchTest regexTest = new DescriptionRegexMatchTest();
        NativeIdSetBI results = regexTest.getQuery().compute();
        assertEquals(REPORTS.getQueryCount("DescriptionRegexMatchTest"), results.size());
    }

    @Test
    public void testDifferenceQuery() throws IOException, Exception {
        XorVersionTest xorTest = new XorVersionTest();
        NativeIdSetBI results = xorTest.computeQuery();
        LOGGER.log(Level.INFO, "Different query size: {0}", results.size());
        assertEquals(REPORTS.getQueryCount("Xor(ConceptIsKindOf('disease'), ConceptIsKindOf('disease', 'v2'))"), results.size());
    }

    @Test
    public void testConceptIsKindOfVersioned() throws IOException, Exception {
        LOGGER.log(Level.INFO, "Test ConceptIsKindOf versioned");
        final SetViewCoordinate d = new SetViewCoordinate(2002, 1, 31, 0, 0);
        Query q = new Query(d.getViewCoordinate()) {

            @Override
            protected NativeIdSetBI For() throws IOException {
                return Ts.get().getAllConceptNids();
            }

            @Override
            public void Let() throws IOException {
                let("motion", Snomed.MOTION);
                let("v2", d.getViewCoordinate());
            }

            @Override
            public Clause Where() {
                return Or(ConceptIsKindOf("motion", "v2"));
            }
        };
        NativeIdSetBI results = q.compute();
        LOGGER.log(Level.INFO, d.v1.toString());
        NativeIdSetItrBI setBitIterator = results.getSetBitIterator();
        while (setBitIterator.next()) {
            LOGGER.log(Level.INFO, Ts.get().getConcept(setBitIterator.nid()).toLongString());
        }
        assertEquals(REPORTS.getQueryCount("ConceptIsKindOfVersionedTest"), results.size());
    }

    @Test
    public void testConceptIs() throws IOException, Exception {
        ConceptIsTest test = new ConceptIsTest();
        NativeIdSetBI results = test.computeQuery();
        assertEquals(REPORTS.getQueryCount("ConceptIsVersionedTest"), results.size());
        for (Object o : test.q.returnDisplayObjects(results, ReturnTypes.DESCRIPTION_VERSION_PREFERRED)) {
            DescriptionVersionDdo ddo = (DescriptionVersionDdo) o;
            assertEquals("Motion", ddo.getText());
            assertTrue(ddo.getComponentNid() == Ts.get().getConceptVersion(test.q.getViewCoordinate(), Snomed.MOTION.getNid()).getPreferredDescription().getNid());
        }
    }

    @Test
    public void testDescriptionLuceneMatch() throws IOException, Exception {
        DescriptionLuceneMatchTest descLuceneMatch = new DescriptionLuceneMatchTest();
        NativeIdSetBI results = descLuceneMatch.computeQuery();

        Set<Long> longIds = QueryTest.REPORTS.getQuerySet("DescriptionLuceneMatch test");
        Set<ComponentChronicleBI> components = this.getComponentsFromSnomedIds(longIds);
        NativeIdSetBI reportSet = new ConcurrentBitSet();
        for (ComponentChronicleBI c : components) {
            reportSet.add(c.getNid());
            DescriptionVersionBI dv = (DescriptionVersionBI) c;
            LOGGER.log(Level.INFO, "DescriptionLuceneMatch report set: {0}", dv.getText());
            LOGGER.log(Level.INFO, "Description status: {0}", dv.getStatus());
        }

        NativeIdSetBI resultsCopy = new ConcurrentBitSet();
        resultsCopy.or(results);

        resultsCopy.xor(reportSet);
        NativeIdSetItrBI iter = resultsCopy.getSetBitIterator();
        while (iter.next()) {
            LOGGER.log(Level.INFO, "DescriptionLuceneMatch xor: {0}", Ts.get().getComponentVersion(VC_LATEST_ACTIVE_AND_INACTIVE, iter.nid()).toUserString());
            LOGGER.log(Level.INFO, "The nid is in the OTF set: {0}", results.contains(iter.nid()));
            LOGGER.log(Level.INFO, "The nid is in the mojo set: {0}", reportSet.contains(iter.nid()));
        }

        LOGGER.log(Level.INFO, "Description Lucene match test size: {0}", results.size());
        assertEquals(REPORTS.getQueryCount("DescriptionLuceneMatch('Oligophrenia')"), results.size());
    }

    @Test
    public void testOr() throws IOException, Exception {
        Query q = new Query() {
            @Override
            protected NativeIdSetBI For() throws IOException {
                return Ts.get().getAllConceptNids();
            }

            @Override
            public void Let() throws IOException {
                let("motion", Snomed.MOTION);
                let("acceleration", Snomed.ACCELERATION);
            }

            @Override
            public Clause Where() {
                return Or(ConceptIs("motion"),
                        ConceptIs("acceleration"));
            }
        };

        NativeIdSetBI results = q.compute();
        assertEquals(REPORTS.getQueryCount("OrTest"), results.size());
    }

    @Test
    public void testXor() throws IOException, Exception {

        Query q = new Query(VC_LATEST_ACTIVE_ONLY) {
            @Override
            protected NativeIdSetBI For() throws IOException {
                return Ts.get().getAllConceptNids();
            }

            @Override
            public void Let() throws IOException {
                let("Acceleration", Snomed.ACCELERATION);
                let("Motion", Snomed.MOTION);
            }

            @Override
            public Clause Where() {
                return Xor(ConceptIsDescendentOf("Acceleration"),
                        ConceptIsKindOf("Motion"));
            }
        };

        NativeIdSetBI results = q.compute();
        LOGGER.log(Level.INFO, "Xor result size: {0}", results.size());
        assertEquals(REPORTS.getQueryCount("XorTest"), results.size());
    }

    @Test
    public void testPreferredTerm() throws IOException, Exception {
        LOGGER.log(Level.INFO, "Sequence: {0}", Ts.get().getSequence());
        PreferredNameForConceptTest preferredNameTest = new PreferredNameForConceptTest();
        NativeIdSetBI results = preferredNameTest.computeQuery();
        LOGGER.log(Level.INFO, "Preferred query result count: {0}", results.size());
        for (Object o : preferredNameTest.getQuery().returnDisplayObjects(results, ReturnTypes.UUIDS)) {
            LOGGER.log(Level.INFO, "Preferred description: {0}", o.toString());
        }
        assertEquals(REPORTS.getQueryCount("PreferredTermTest"), results.size());
    }

    @Test
    public void testRelType() throws IOException, Exception {

        RelTypeTest relTest = new RelTypeTest();
        NativeIdSetBI results = relTest.getQuery().compute();
        LOGGER.log(Level.INFO, "Relationship test: {0}", results.size());
//        Set<Long> querySet = REPORTS.getQuerySet("RelTypeTest set");
//        Set<RelationshipChronicleBI> rels = new HashSet<>();
//        for (Long l : querySet) {
//            String resultUUIDString = target("alternate-id/uuid/" + l).request(MediaType.TEXT_PLAIN).get(String.class);
//            RelationshipChronicleBI rel = (RelationshipChronicleBI) Ts.get().getComponent(UUID.fromString(resultUUIDString));
//            rels.add(rel);
//        }
//
//        ViewCoordinate vc = VC_LATEST_ACTIVE_ONLY;
//
//        NativeIdSetBI kindOfFindingSite = Ts.get().isKindOfSet(Snomed.FINDING_SITE.getNid(), vc);
//        NativeIdSetBI kindOfEndocrine = Ts.get().isKindOfSet(Snomed.STRUCTURE_OF_ENDOCRINE_SYSTEM.getNid(), vc);
//
//        NativeIdSetBI exp = new ConcurrentBitSet();
//        for (RelationshipChronicleBI r : rels) {
//            RelationshipVersionBI relVersion = r.getVersion(VC_LATEST_ACTIVE_ONLY);
//            if (relVersion != null) {
//                int destinationNid = relVersion.getDestinationNid();
//                int typeId = relVersion.getTypeNid();
//
//                boolean dest = kindOfEndocrine.contains(destinationNid);
//                boolean type = kindOfFindingSite.contains(typeId);
//                if (!dest) {
//                    LOGGER.log(Level.SEVERE, "Relationship not in destinaton set: {0}", relVersion.toString());
//                }
//                if (!type) {
//                    LOGGER.log(Level.SEVERE, "Relationship not in type set: {0}", relVersion.toString());
//                }
//                exp.add(relVersion.getNid());
//                assertTrue(kindOfFindingSite.contains(typeId));
//                assertTrue(kindOfEndocrine.contains(destinationNid));
//            }
//        }
        assertEquals(REPORTS.getQueryCount("RelType('Finding site', 'Structure of endocrine system')"), results.size());
    }

    @Test
    public void testRelTypeVersioning() throws IOException, Exception {
        final SetViewCoordinate setViewCoordinate = new SetViewCoordinate(2002, 1, 31, 0, 0);
        Query q = new Query(VC_LATEST_ACTIVE_ONLY) {
            @Override
            protected NativeIdSetBI For() throws IOException {
                return Ts.get().getAllConceptNids();
            }

            @Override
            public void Let() throws IOException {
                let("endocrine system", Snomed.STRUCTURE_OF_ENDOCRINE_SYSTEM);
                let("finding site", Snomed.FINDING_SITE);
                let("v2", setViewCoordinate.getViewCoordinate());
            }

            @Override
            public Clause Where() {
                return And(RelType("finding site", "endocrine system"), AndNot(RelType("finding site", "endocrine system", "v2")));
            }
        };

        NativeIdSetBI results = q.compute();
        assertEquals(REPORTS.getQueryCount("RelType versioning test"), results.size());
    }

    @Test
    public void testRelRestrictionSubsumptionTrue() throws IOException, Exception {
        LOGGER.log(Level.INFO, "Rel restriction subsumption true");
        RelRestrictionTest rrTest = new RelRestrictionTest();
        NativeIdSetBI results = rrTest.computeQuery();
        for (Object o : rrTest.q.returnDisplayObjects(results, ReturnTypes.COMPONENT)) {
            LOGGER.log(Level.INFO, o.toString());
        }

        NativeIdSetBI laserSurgerySet = Ts.get().isKindOfSet(Snomed.LASER_SURGERY.getNid(), VC_LATEST_ACTIVE_ONLY);
        NativeIdSetBI destinationSet = Ts.get().isKindOfSet(Snomed.EYE_STRUCTURE.getNid(), VC_LATEST_ACTIVE_ONLY);
        NativeIdSetBI relTypeSet = Ts.get().isKindOfSet(Snomed.PROCEDURE_SITE.getNid(), VC_LATEST_ACTIVE_ONLY);

        NativeIdSetItrBI iter = results.getSetBitIterator();
        Set<ConceptVersionBI> relResults = new HashSet<>();
        while (iter.next()) {
            ConceptChronicleBI concept = Ts.get().getConcept(iter.nid());
            relResults.add(concept.getVersion(VC_LATEST_ACTIVE_ONLY));
        }

        NativeIdSetBI conceptSet = new ConcurrentBitSet();
        for (ConceptVersionBI concept : relResults) {
            for (RelationshipVersionBI version : concept.getRelationshipsOutgoingActive()) {
                int sourceId = version.getConceptNid();
                int typeNid = version.getTypeNid();
                int destinationId = version.getDestinationNid();
                if (laserSurgerySet.contains(sourceId) && destinationSet.contains(destinationId) && relTypeSet.contains(typeNid) && version.isActive()) {
                    conceptSet.add(sourceId);
                }
            }
        }

        conceptSet.and(results);
        LOGGER.log(Level.INFO, "Concept AND size: {0}", conceptSet.size());

//        Set<Long> querySet = REPORTS.getQuerySet("RelRestriction test set");
//        Set<RelationshipChronicleBI> rels = new HashSet<>();
//        for (Long l : querySet) {
//            String resultUUIDString = target("alternate-id/uuid/" + l).request(MediaType.TEXT_PLAIN).get(String.class);
//            RelationshipChronicleBI rel = (RelationshipChronicleBI) Ts.get().getComponent(UUID.fromString(resultUUIDString));
//            rels.add(rel);
//        }
//        
//        NativeIdSetBI laserSurgerySet = Ts.get().isKindOfSet(Snomed.LASER_SURGERY.getNid(), StandardViewCoordinates.getSnomedInferredLatest());
//        NativeIdSetBI destinationSet = Ts.get().isKindOfSet(Snomed.EYE_STRUCTURE.getNid(), StandardViewCoordinates.getSnomedInferredLatest());
//        NativeIdSetBI relTypeSet = Ts.get().isKindOfSet(Snomed.PROCEDURE_SITE.getNid(), StandardViewCoordinates.getSnomedInferredLatest());
//        
//        for (RelationshipChronicleBI r : rels) {
//            RelationshipVersionBI version = r.getVersion(StandardViewCoordinates.getSnomedInferredLatest());
//            int sourceId = version.getConceptNid();
//            int typeNid = version.getTypeNid();
//            int destinationId = version.getDestinationNid();
//            assertTrue(laserSurgerySet.contains(sourceId));
//            assertTrue(destinationSet.contains(destinationId));
//            assertTrue(relTypeSet.contains(typeNid));
//            if (!version.isActive()) {
//                LOGGER.log(Level.SEVERE, "Relationship is in result set but is not active: {0}", version.toString());
//            }
//            //assertTrue(version.isActive());
//        }
        assertEquals(REPORTS.getQueryCount("RelRestriction test"), results.size());
    }

    @Test
    public void testRelRestrictionSubsumptionFalse() throws IOException, Exception {
        LOGGER.log(Level.INFO, "RelRestriction subsumption false test");
        RelRestriction2Test test = new RelRestriction2Test();
        NativeIdSetBI results = test.computeQuery();
        assertEquals(REPORTS.getQueryCount("RelRestrictionTest2"), results.size());
    }

    @Test
    public void testRelRestrictionSubsumptionNull() throws IOException, Exception {
        Query q = new Query(VC_LATEST_ACTIVE_ONLY) {
            @Override
            protected NativeIdSetBI For() throws IOException {
                return Ts.get().getAllConceptNids();
            }

            @Override
            public void Let() throws IOException {
                let("physical force", Snomed.PHYSICAL_FORCE);
                let("is a", Snomed.IS_A);
                let("motion", Snomed.MOTION);
            }

            @Override
            public Clause Where() {
                return Or(RelRestriction("motion", "is a", "physical force"));
            }
        };
        NativeIdSetBI results = q.compute();
        int[] setValues = results.getSetValues();
        int count = 0;
        for (Object o : q.returnDisplayObjects(results, ReturnTypes.NIDS)) {
            assertEquals(setValues[count], Integer.parseInt(o.toString()));
            count++;
        }
        LOGGER.log(Level.INFO, "Rel restriction subsumption null results: {0}", results.size());
        assertEquals(7, results.size());

    }

    @Test
    public void testFullySpecifiedName() throws IOException, Exception {
        FullySpecifiedNameForConceptTest fsnTest = new FullySpecifiedNameForConceptTest();
        NativeIdSetBI results = fsnTest.computeQuery();
        LOGGER.log(Level.INFO, "Fully specified name test: {0}", results.size());
        for (Object o : fsnTest.getQuery().returnDisplayObjects(results, ReturnTypes.UUIDS)) {
            LOGGER.log(Level.INFO, o.toString());
        }
        int queryResultSize = REPORTS.getQueryCount("FSNTest");
        assertEquals(queryResultSize, results.size());
        assertEquals(queryResultSize, fsnTest.getQuery().returnDisplayObjects(results, ReturnTypes.NIDS).size());
    }

    @Test
    public void testAnd() throws IOException, Exception {
        AndTest andTest = new AndTest();
        NativeIdSetBI results = andTest.computeQuery();
        LOGGER.log(Level.INFO, "And query test results: {0}", results.size());
        assertEquals(REPORTS.getQueryCount("AndTest"), results.size());
    }

    @Test
    public void isChildOfTest() throws IOException, Exception {
        IsChildOfTest isChildOfTest = new IsChildOfTest();
        Query q3 = isChildOfTest.getQuery();
        q3.getViewCoordinate().setAllowedStatus(EnumSet.of(Status.ACTIVE));
        NativeIdSetBI results3 = q3.compute();
        LOGGER.log(Level.INFO, "Query result count {0}", results3.size());

//        Set<Long> querySet = REPORTS.getQuerySet("ConceptIsChildOfTest set");
//        Set<ConceptChronicleBI> concepts = new HashSet<>();
//
//        for (Long l : querySet) {
//            String resultUUIDString = target("alternate-id/uuid/" + l).request(MediaType.TEXT_PLAIN).get(String.class
//            );
//            ConceptChronicleBI concept = Ts.get().getConcept(UUID.fromString(resultUUIDString));
//
//            LOGGER.log(Level.INFO, concept.toLongString());
//            concepts.add(concept);
//        }
//
//        NativeIdSetBI resultsFromMojo = new ConcurrentBitSet();
//        for (ConceptChronicleBI c : concepts) {
//            resultsFromMojo.add(c.getConceptNid());
//        }
//
//        NativeIdSetBI resultsDifference = new ConcurrentBitSet();
//        resultsDifference.or(results3);
//        resultsDifference.andNot(resultsFromMojo);
//
//        resultsDifference.xor(resultsFromMojo);
//        NativeIdSetItrBI setBitIterator = resultsDifference.getSetBitIterator();
//        LOGGER.log(Level.INFO, "Differences between the set values in ChildOfTest:");
//        while (setBitIterator.next()) {
//            ConceptChronicleBI cc = Ts.get().getConcept(setBitIterator.nid());
//            LOGGER.log(Level.INFO, cc.toLongString());
//        }
        assertEquals(REPORTS.getQueryCount("ConceptIsChildOfTest"), results3.size());
    }

    @Test
    public void isDescendentOfTest() throws IOException, Exception {
        IsDescendentOfTest isDescendent = new IsDescendentOfTest();
        Query q4 = isDescendent.getQuery();
        NativeIdSetBI results4 = q4.compute();
        LOGGER.log(Level.INFO, "ConceptIsDescendentOf query result count {0}", results4.size());
        assertEquals(REPORTS.getQueryCount("ConceptIsDescendentOfTest"), results4.size());
    }

    @Test
    public void isKindOfTest() throws IOException, Exception {
        IsKindOfTest kindOf = new IsKindOfTest();
        Query kindOfQuery = kindOf.getQuery();
        NativeIdSetBI kindOfResults = kindOfQuery.compute();
        NativeIdSetItrBI iter = kindOfResults.getSetBitIterator();
        while (iter.next()) {
            assertTrue(Ts.get().isKindOf(iter.nid(), Snomed.PHYSICAL_FORCE.getNid(), VC_LATEST_ACTIVE_ONLY));
        }

        assertEquals(REPORTS.getQueryCount("IsKindOfTest"), kindOfResults.size());
    }

    @Test
    public void queryTest() throws IOException, Exception {
        Query q = new Query(VC_LATEST_ACTIVE_AND_INACTIVE) {
            @Override
            protected NativeIdSetBI For() throws IOException {
                return Ts.get().isKindOfSet(Snomed.MOTION.getNid(), VC_LATEST_ACTIVE_AND_INACTIVE);
            }

            @Override
            public void Let() throws IOException {
                let("acceleration", Snomed.ACCELERATION);
                let("is a", Snomed.IS_A);
                let("motion", Snomed.MOTION);
                let("deceleration", "[Dd]eceleration.*");
                let("continued movement", Snomed.CONTINUED_MOVEMENT);
                let("centrifugal", "[CC]entrifugal.*");
            }

            @Override
            public Clause Where() {
                return And(ConceptForComponent(DescriptionRegexMatch("deceleration")),
                        And(Or(RelType("is a", "motion"),
                                        ConceptForComponent(DescriptionRegexMatch("centrifugal"))),
                                ConceptIsKindOf("motion"),
                                Not(Or(ConceptIsChildOf("acceleration"),
                                                ConceptIs("continued movement")))));
            }
        };

        NativeIdSetBI results = q.compute();
        LOGGER.log(Level.INFO, "Query test results: {0}", results.size());
        for (Object o : q.returnDisplayObjects(results, ReturnTypes.UUIDS)) {
            LOGGER.log(Level.INFO, o.toString());
        }
        assertEquals(REPORTS.getQueryCount("QueryTest"), results.size());

    }

    @Test
    public void notTest() throws IOException, Exception {
        Query q = new Query() {
            @Override
            protected NativeIdSetBI For() throws IOException {
                return Ts.get().getAllConceptNids();
            }

            @Override
            public void Let() throws IOException {
                let("acceleration", Snomed.ACCELERATION);

            }

            @Override
            public Clause Where() {
                return Not(ConceptIs("acceleration"));
            }
        };

        NativeIdSetBI results = q.compute();
        LOGGER.log(Level.INFO, "Not test result size: {0}", results.size());
        assertEquals(REPORTS.getQueryCount("NotTest"), results.size());
    }

    @Test
    public void conceptForComponentTest() throws IOException, Exception {
        LOGGER.log(Level.INFO, "ConceptForComponentTest");
        ConceptForComponentTest cfcTest = new ConceptForComponentTest();
        NativeIdSetBI results = cfcTest.computeQuery();
        assertEquals(REPORTS.getQueryCount("ConceptForComponentTest"), results.size());
    }

    @Test
    public void refsetLuceneMatchTest() throws IOException, Exception {
        LOGGER.log(Level.INFO, "RefsetLuceneMatch test");
        RefsetLuceneMatchTest rlmTest = new RefsetLuceneMatchTest();
        NativeIdSetBI ids = rlmTest.computeQuery();
        for (Object o : rlmTest.q.returnDisplayObjects(ids, ReturnTypes.COMPONENT)) {
            LOGGER.log(Level.INFO, o.toString());
            assertTrue(o.toString().contains("Virtual medicinal product simple reference set"));
        }
        assertEquals(1, ids.size());
    }

    @Test
    public void refsetContainsConceptTest() throws IOException, Exception {
        LOGGER.log(Level.INFO, "RefsetContainsConcept test");
        TermstoreChanges tc = new TermstoreChanges(VC_LATEST_ACTIVE_ONLY);
        tc.addRefsetMember();
        RefsetContainsConceptTest rccTest = new RefsetContainsConceptTest();
        NativeIdSetBI ids = rccTest.computeQuery();
        assertEquals(1, ids.size());

    }

    @Test
    public void refsetContainsStringTest() throws Exception {
        LOGGER.log(Level.INFO, "RefsetContainsString test");
        TermstoreChanges tc = new TermstoreChanges(VC_LATEST_ACTIVE_ONLY);
        tc.addRefsetMember();
        RefsetContainsStringTest rcsTest = new RefsetContainsStringTest();
        NativeIdSetBI ids = rcsTest.computeQuery();
        assertEquals(1, ids.size());
    }

    @Test
    public void refsetContainsKindOfConceptTest() throws Exception {
        LOGGER.log(Level.INFO, "RefsetContainsKindOfConcept test");
        TermstoreChanges tc = new TermstoreChanges(VC_LATEST_ACTIVE_ONLY);
        tc.addRefsetMember();
        RefsetContainsKindOfConceptTest rckocTest = new RefsetContainsKindOfConceptTest();
        NativeIdSetBI nids = rckocTest.computeQuery();
        assertEquals(1, nids.size());
    }

    @Test
    public void testOr2() throws IOException, Exception {
        LOGGER.log(Level.INFO, "Or test");
        OrTest orTest = new OrTest();
        NativeIdSetBI results = orTest.computeQuery();
        assertEquals(REPORTS.getQueryCount("OrTest2"), results.size());
    }

    @Test
    public void notTest2() throws IOException, Exception {
        LOGGER.log(Level.INFO, "Not test2");
        NotTest notTest = new NotTest();
        NativeIdSetBI results = notTest.computeQuery();
        assertEquals(REPORTS.getQueryCount("NotTest2"), results.size());
    }

    @Test
    public void ExampleTest() throws Exception {
        LOGGER.log(Level.INFO, "Example test");
        QueryExample test = new QueryExample();

        assertEquals(REPORTS.getQueryCount("ExampleQueryTest"), test.getResults().size());
    }

    @Test
    public void descriptionActiveLuceneMatchTest() throws Exception {
        LOGGER.log(Level.INFO, "DescriptionActiveLuceneMatch test");
        Query q1 = new Query(VC_LATEST_ACTIVE_AND_INACTIVE) {

            @Override
            protected NativeIdSetBI For() throws IOException {
                return Ts.get().getAllConceptNids();
            }

            @Override
            public void Let() throws IOException {
                let("baranys", "barany's");
            }

            @Override
            public Clause Where() {
                return ConceptForComponent(DescriptionLuceneMatch("baranys"));
            }
        };
        NativeIdSetBI results1 = q1.compute();
        assertEquals(1, results1.size());

        TermstoreChanges tc = new TermstoreChanges(VC_LATEST_ACTIVE_AND_INACTIVE);

        for (DescriptionVersionBI desc : Ts.get().getConceptVersion(VC_LATEST_ACTIVE_AND_INACTIVE, Snomed.BARANYS_SIGN.getNid()).getDescriptionsActive()) {
            tc.setActiveStatus(desc, Status.INACTIVE);
        }
        DescriptionActiveLuceneMatchTest test = new DescriptionActiveLuceneMatchTest();
        NativeIdSetBI results2 = test.computeQuery();
        assertEquals(results2.size(), results1.size() - 1);
        for (DescriptionChronicleBI desc : Ts.get().getConceptVersion(VC_LATEST_ACTIVE_AND_INACTIVE, Snomed.BARANYS_SIGN.getNid()).getDescriptions()) {
            DescriptionVersionBI descVersion = desc.getVersion(VC_LATEST_ACTIVE_AND_INACTIVE);
            tc.setActiveStatus(descVersion, Status.ACTIVE);
        }
    }

    @Test
    public void ChangeFromPreviousVersionTest() throws IOException, Exception {
        LOGGER.log(Level.INFO, "Changed from previous version test");
        ChangedFromPreviousVersionTest test = new ChangedFromPreviousVersionTest();
        SetViewCoordinate svc = new SetViewCoordinate(2010, 1, 31, 0, 0);
        ViewCoordinate previousVC = svc.getViewCoordinate();
        TermstoreChanges tc = new TermstoreChanges(previousVC);
        tc.modifyDesc("Admin statuses", Snomed.ADMINISTRATIVE_STATUSES.getNid());
        NativeIdSetBI results2 = test.computeQuery();
        for (Object o : test.q.returnDisplayObjects(results2, ReturnTypes.DESCRIPTION_VERSION_FSN)) {
            LOGGER.log(Level.INFO, o.toString());
        }
        assertEquals(1, results2.size());
    }

    @Test
    public void DescriptionActiveRegexMatchTest() throws IOException, ContradictionException, InvalidCAB, Exception {
        LOGGER.log(Level.INFO, "Description active regex match test");
        TermstoreChanges tc = new TermstoreChanges(VC_LATEST_ACTIVE_AND_INACTIVE);
        for (DescriptionVersionBI desc : Ts.get().getConceptVersion(VC_LATEST_ACTIVE_AND_INACTIVE, Snomed.ACCELERATION.getNid()).getDescriptionsActive()) {
            tc.setActiveStatus(desc, Status.INACTIVE);
        }
        DescriptionActiveRegexMatchTest test = new DescriptionActiveRegexMatchTest();
        try {
            NativeIdSetBI computeQuery = test.computeQuery();
            NativeIdSetItrBI iter = computeQuery.getSetBitIterator();
            while (iter.next()) {
                LOGGER.log(Level.INFO, "DescriptionActiveRegexMatch test: {0}", Ts.get().getComponentVersion(VC_LATEST_ACTIVE_ONLY, iter.nid()));
            }

//            Set<ComponentChronicleBI> concepts = this.getComponentsFromSnomedIds(REPORTS.getQuerySet("DescriptionActiveRegexMatchTest set"));
//            for (ComponentChronicleBI c : concepts) {
//                LOGGER.log(Level.INFO, "DescriptionActiveRegexMatch set: {0}", c.toUserString());
//            }
            //Since the reports doesn't account for the fact that descriptions for "Accleration" have been made inactive within the tests,
            //the result set from the report will still include "Acceleration"
            int expectedCardinality = REPORTS.getQueryCount("DescriptionActiveRegexMatchTest") - 1;
            LOGGER.log(Level.INFO, "DescriptionActiveRegexMAtch test size: {0}", computeQuery.size());
            assertEquals(expectedCardinality, computeQuery.size());
        } catch (Exception ex) {
            LOGGER.log(Level.INFO, null, ex);
            fail("DescriptionActiveRegexMatchTest failed and threw: " + ex.toString());
        } finally {
            for (DescriptionChronicleBI desc : Ts.get().getConceptVersion(VC_LATEST_ACTIVE_AND_INACTIVE, Snomed.ACCELERATION.getNid()).getDescriptions()) {
                DescriptionVersionBI descVersion = desc.getVersion(VC_LATEST_ACTIVE_AND_INACTIVE);
                tc.setActiveStatus(descVersion, Status.ACTIVE);
            }
        }
    }

    @Test
    public void testKindOfDiseaseVersioned() throws Exception {
        final SetViewCoordinate setVC = new SetViewCoordinate(2002, 1, 31, 0, 0);
        Query q = new Query(setVC.getViewCoordinate()) {

            @Override
            protected NativeIdSetBI For() throws IOException {
                return Ts.get().getAllConceptNids();
            }

            @Override
            public void Let() throws IOException {
                let("Disease", Snomed.DISEASE);
                let("v2", setVC.getViewCoordinate());
            }

            @Override
            public Clause Where() {
                return Or(ConceptIsKindOf("Disease", "v2"));
            }
        };

        NativeIdSetBI results = q.compute();

//        Set<Long> querySet = REPORTS.getQuerySet("ConceptIsKindOf disease v2");
//        Set<ComponentChronicleBI> componentsFromSnomedIds = this.getComponentsFromSnomedIds(querySet);
//        NativeIdSetBI resultsFromMojo = new ConcurrentBitSet();
//        for (ComponentChronicleBI c : componentsFromSnomedIds) {
//            resultsFromMojo.add(c.getVersion(setVC.getViewCoordinate()).getNid());
//        }
//
//        results.andNot(resultsFromMojo);
//        LOGGER.log(Level.INFO, "Size of the result set from Mojo: {0}", resultsFromMojo.size());
//        LOGGER.log(Level.INFO, "Size of the result set from OTF and not Mojo set: {0}", results.size());
//        iter = results.getSetBitIterator();
//        int count = 0;
//        while (iter.next() && count < 100) {
//            if (resultsFromMojo.contains(iter.nid())) {
//                fail("AndNot operation not performed successfully.");
//            }
//            ConceptChronicleBI concept = Ts.get().getConcept(iter.nid());
//            ConceptVersionBI version = concept.getVersion(setVC.getViewCoordinate());
//            LOGGER.log(Level.INFO, version.toLongString());
//            assertTrue(version.isActive());
//            assertTrue(version.isKindOf(disease));
//            count++;
//        }
        int exp = REPORTS.getQueryCount("ConceptIsKindOf('disease', 'v2')");
        assertEquals(exp, results.size());
    }

//    @Test
//    public void testConceptIsKindOfAcuteAllergicReactionVersioned() throws Exception {
//        LOGGER.log(Level.INFO, "ConceptIsKindOf Acute allergic reaction");
//        final SetViewCoordinate setVC = new SetViewCoordinate(2008, 1, 31, 0, 0);
//        Query q = new Query(setVC.getViewCoordinate()) {
//
//            @Override
//            protected NativeIdSetBI For() throws IOException {
//                return Ts.get().getAllConceptNids();
//            }
//
//            @Override
//            public void Let() throws IOException {
//                let("v2", setVC.getViewCoordinate());
//                let("Acute allergic reaction", Snomed.ACUTE_ALLERGIC_REACTION);
//            }
//
//            @Override
//            public Clause Where() {
//                return Or(ConceptIsKindOf("Acute allergic reaction", "v2"));
//            }
//        };
//
//        NativeIdSetBI resultsFromOTF = q.compute();
//        assertEquals(REPORTS.getQueryCount("ConceptIsKindOf Acute allergic reaction versioned"), resultsFromOTF.size());
//    }

//    @Test
//    public void testKindOfVenomInducedAnaphylaxis() throws IOException, Exception {
//        final SetViewCoordinate setVC = new SetViewCoordinate(2008, 1, 31, 0, 0);
//        Query q = new Query(setVC.getViewCoordinate()) {
//
//            @Override
//            protected NativeIdSetBI For() throws IOException {
//                return Ts.get().getAllConceptNids();
//            }
//
//            @Override
//            public void Let() throws IOException {
//                let("venom-induced anaphylaxis", Snomed.VENOM_INDUCED_ANAPHYLAXIS);
//                let("v2", setVC.getViewCoordinate());
//            }
//
//            @Override
//            public Clause Where() {
//                return Or(ConceptIsKindOf("venom-induced anaphylaxis", "v2"));
//            }
//        };
//
//        NativeIdSetBI resultsFromOTF = q.compute();
//
//        NativeIdSetBI resultsFromMojo = new ConcurrentBitSet();
//        Set<Long> querySet = REPORTS.getQuerySet("KindOf Venom-induced anaphylaxis set");
//        Set<ComponentChronicleBI> components = this.getComponentsFromSnomedIds(querySet);
//        for (ComponentChronicleBI component : components) {
//            resultsFromMojo.add(component.getNid());
//        }
//
//        resultsFromOTF.andNot(resultsFromMojo);
//        LOGGER.log(Level.INFO, "OTF kind of Venom-induced anaphylaxis and not Mojo set: {0}", resultsFromOTF.size());
//        NativeIdSetItrBI iter = resultsFromOTF.getSetBitIterator();
//        while (iter.next()) {
//            LOGGER.log(Level.INFO, "OTF kind of Venom-induced anaphylaxis: {0}", Ts.get().getComponent(iter.nid()));
//        }
//
//    }

    private Set<ComponentChronicleBI> getComponentsFromSnomedIds(Set<Long> querySet) throws IOException {
        Set<ComponentChronicleBI> components = new HashSet<>();
        ComponentChronicleBI component;
        String resultUUIDString;
        for (Long l : querySet) {
            resultUUIDString = target("alternate-id/uuid/" + l).request(MediaType.TEXT_PLAIN).get(String.class);
            if (resultUUIDString != null) {
                component = Ts.get().getComponent(UUID.fromString(resultUUIDString));
                components.add(component);
            }
        }
        return components;
    }
}
