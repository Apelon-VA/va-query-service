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
import gov.vha.isaac.metadata.coordinates.ViewCoordinates;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.ihtsdo.otf.query.implementation.*;
import org.ihtsdo.otf.query.integration.tests.rest.TermstoreChanges;
import org.ihtsdo.otf.tcc.api.blueprint.InvalidCAB;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.api.concept.ConceptVersionBI;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.description.DescriptionChronicleBI;
import org.ihtsdo.otf.tcc.api.description.DescriptionVersionBI;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;
import org.ihtsdo.otf.tcc.api.nid.ConcurrentBitSet;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetItrBI;
import org.ihtsdo.otf.tcc.api.relationship.RelationshipVersionBI;
import org.ihtsdo.otf.tcc.api.store.Ts;
import org.ihtsdo.otf.tcc.model.cc.PersistentStore;
import static org.testng.Assert.*;
import org.testng.annotations.*;

/**
 * Class that handles integration tests for
 * {@link org.ihtsdo.otf.query.implementation.Clause} implementations.
 *
 * @author kec
 */
public class QueryTest {

    private static final String DIR = System.getProperty("user.dir");
    private static final JSONToReport REPORTS = new JSONToReport(DIR + "/target/test-resources/OTFReports.json");
    private static final JSONToReport CEMENT_REPORT = new JSONToReport(DIR + "/target/test-resources/eConceptReports.json");
    private static final Logger LOGGER = Logger.getLogger(QueryTest.class.getName());
    private static ViewCoordinate VC_LATEST_ACTIVE_AND_INACTIVE;
    private static ViewCoordinate VC_LATEST_ACTIVE_ONLY;

    //private static int cementSize;

    public QueryTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        REPORTS.parseFile();
        CEMENT_REPORT.parseFile();
        //cementSize = CEMENT_REPORT.getQueryCount("cement.jbin concept set size");
        //LOGGER.log(Level.INFO, "Cement size: {0}", cementSize);
        try {
            VC_LATEST_ACTIVE_AND_INACTIVE = ViewCoordinates.getDevelopmentInferredLatest();
            VC_LATEST_ACTIVE_ONLY = ViewCoordinates.getDevelopmentInferredLatestActiveOnly();
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        }
    }

    @Test(groups = "QueryServiceTests")
    public void testSimpleQuery() throws IOException, Exception {
        LOGGER.log(Level.INFO, "Simple query: ");
        Query q = new Query() {

            @Override
            protected ForSetSpecification ForSetSpecification() throws IOException {
                ForSetSpecification forSetSpecification = new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
                return forSetSpecification;
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

    @Test(groups = "QueryServiceTests")
    public void testRegexQuery() throws IOException, Exception {
        DescriptionRegexMatchTest regexTest = new DescriptionRegexMatchTest();
        NativeIdSetBI results = regexTest.getQuery().compute();
        assertEquals(REPORTS.getQueryCount("DescriptionRegexMatchTest"), results.size());
    }

    @Test(groups = "QueryServiceTests")
    public void testDifferenceQuery() throws IOException, Exception {
        XorVersionTest xorTest = new XorVersionTest();
        NativeIdSetBI results = xorTest.computeQuery();
        LOGGER.log(Level.INFO, "Different query size: {0}", results.size());
        assertEquals(REPORTS.getQueryCount("Xor(ConceptIsKindOf('disease'), ConceptIsKindOf('disease', 'v2'))"), results.size());
    }

    @Test(groups = "QueryServiceTests")
    public void testConceptIsKindOfVersioned() throws IOException, Exception {
        LOGGER.log(Level.INFO, "Test ConceptIsKindOf versioned");
        final SetViewCoordinate d = new SetViewCoordinate(2002, 1, 31, 0, 0);
        Query q = new Query(d.getViewCoordinate()) {

            @Override
            protected ForSetSpecification ForSetSpecification() throws IOException {
                ForSetSpecification forSetSpecification = new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
                return forSetSpecification;
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
            LOGGER.log(Level.INFO, PersistentStore.get().getConcept(setBitIterator.nid()).toLongString());
        }
        assertEquals(7, results.size());
        //assertEquals(REPORTS.getQueryCount("ConceptIsKindOfVersionedTest"), results.size());
    }

    @Test(groups = "QueryServiceTests")
    public void testConceptIs() throws IOException, Exception {
        ConceptIsTest test = new ConceptIsTest();
        NativeIdSetBI results = test.computeQuery();
        assertEquals(REPORTS.getQueryCount("ConceptIsVersionedTest"), results.size());
    }

    @Test(enabled = false)
    public void testDescriptionLuceneMatch() throws IOException, Exception {
        DescriptionLuceneMatchTest descLuceneMatch = new DescriptionLuceneMatchTest();
        NativeIdSetBI results = descLuceneMatch.computeQuery();

        LOGGER.log(Level.INFO, "Description Lucene match test size: {0}", results.size());
        assertEquals(REPORTS.getQueryCount("DescriptionLuceneMatch('Oligophrenia')"), results.size());
    }

    @Test(groups = "QueryServiceTests")
    public void testOr() throws IOException, Exception {
        Query q = new Query() {
            @Override
            protected ForSetSpecification ForSetSpecification() throws IOException {
                ForSetSpecification forSetSpecification = new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
                return forSetSpecification;
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

    @Test(groups = "QueryServiceTests")
    public void testXor() throws IOException, Exception {

        Query q = new Query(VC_LATEST_ACTIVE_ONLY) {
            @Override
            protected ForSetSpecification ForSetSpecification() throws IOException {
                ForSetSpecification forSetSpecification = new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
                return forSetSpecification;
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

    @Test(groups = "QueryServiceTests")
    public void testPreferredTerm() throws IOException, Exception {
        LOGGER.log(Level.INFO, "Sequence: {0}", PersistentStore.get().getSequence());
        PreferredNameForConceptTest preferredNameTest = new PreferredNameForConceptTest();
        NativeIdSetBI results = preferredNameTest.computeQuery();
        LOGGER.log(Level.INFO, "Preferred query result count: {0}", results.size());
        for (Object o : preferredNameTest.getQuery().returnDisplayObjects(results, ReturnTypes.UUIDS)) {
            LOGGER.log(Level.INFO, "Preferred description: {0}", o.toString());
        }
        assertEquals(REPORTS.getQuerySet("PreferredTermTest set").size(), results.size());
    }


    @Test(groups = "QueryServiceTests")
    public void testRelTypeVersioning() throws IOException, Exception {
        final SetViewCoordinate setViewCoordinate = new SetViewCoordinate(2002, 1, 31, 0, 0);
        Query q = new Query(VC_LATEST_ACTIVE_ONLY) {
            @Override
            protected ForSetSpecification ForSetSpecification() throws IOException {
                ForSetSpecification forSetSpecification = new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
                return forSetSpecification;
            }
            @Override
            public void Let() throws IOException {
                let("endocrine system", Snomed.STRUCTURE_OF_ENDOCRINE_SYSTEM);
                let("finding site", Snomed.FINDING_SITE);
                let("v2", setViewCoordinate.getViewCoordinate());
            }

            @Override
            public Clause Where() {
                return And(RelRestriction("finding site", "endocrine system"), 
                        AndNot(RelRestriction("finding site", "endocrine system", "v2")));
            }
        };

        NativeIdSetBI results = q.compute();
        assertEquals(REPORTS.getQueryCount("RelType versioning test"), results.size());
    }

    @Test(groups = "QueryServiceTests")
    public void testRelRestrictionSubsumptionTrue() throws IOException, Exception {
        LOGGER.log(Level.INFO, "Rel restriction subsumption true");
        RelRestrictionTest rrTest = new RelRestrictionTest();
        NativeIdSetBI results = rrTest.computeQuery();
        for (Object o : rrTest.q.returnDisplayObjects(results, ReturnTypes.COMPONENT)) {
            LOGGER.log(Level.INFO, o.toString());
        }

        NativeIdSetBI laserSurgerySet = PersistentStore.get().isKindOfSet(Snomed.LASER_SURGERY.getNid(), VC_LATEST_ACTIVE_ONLY);
        NativeIdSetBI destinationSet = PersistentStore.get().isKindOfSet(Snomed.EYE_STRUCTURE.getNid(), VC_LATEST_ACTIVE_ONLY);
        NativeIdSetBI relTypeSet = PersistentStore.get().isKindOfSet(Snomed.PROCEDURE_SITE.getNid(), VC_LATEST_ACTIVE_ONLY);

        NativeIdSetItrBI iter = results.getSetBitIterator();
        Set<ConceptVersionBI> relResults = new HashSet<>();
        while (iter.next()) {
            ConceptChronicleBI concept = PersistentStore.get().getConcept(iter.nid());
            concept.getVersion(VC_LATEST_ACTIVE_ONLY).ifPresent((conVersion) -> relResults.add(conVersion));
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
//            RelationshipChronicleBI rel = (RelationshipChronicleBI) PersistentStore.get().getComponent(UUID.fromString(resultUUIDString));
//            rels.add(rel);
//        }
//        
//        NativeIdSetBI laserSurgerySet = PersistentStore.get().isKindOfSet(Snomed.LASER_SURGERY.getNid(), StandardViewCoordinates.getSnomedInferredLatest());
//        NativeIdSetBI destinationSet = PersistentStore.get().isKindOfSet(Snomed.EYE_STRUCTURE.getNid(), StandardViewCoordinates.getSnomedInferredLatest());
//        NativeIdSetBI relTypeSet = PersistentStore.get().isKindOfSet(Snomed.PROCEDURE_SITE.getNid(), StandardViewCoordinates.getSnomedInferredLatest());
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

    @Test(groups = "QueryServiceTests")
    public void testRelRestrictionSubsumptionFalse() throws IOException, Exception {
        LOGGER.log(Level.INFO, "RelRestriction subsumption false test");
        RelRestriction2Test test = new RelRestriction2Test();
        NativeIdSetBI results = test.computeQuery();
        assertEquals(REPORTS.getQueryCount("RelRestrictionTest2"), results.size());
    }

    @Test(groups = "QueryServiceTests")
    public void testRelRestrictionSubsumptionNull() throws IOException, Exception {
        Query q = new Query(VC_LATEST_ACTIVE_ONLY) {
            @Override
            protected ForSetSpecification ForSetSpecification() throws IOException {
                ForSetSpecification forSetSpecification = new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
                return forSetSpecification;
            }
            @Override
            public void Let() throws IOException {
                let("physical force", Snomed.PHYSICAL_FORCE);
                let("is a", Snomed.IS_A);
                let("motion", Snomed.MOTION);
                let("relTypeSubsumptionKey", true);
                let("destinationSubsumptionKey", true);
            }

            @Override
            public Clause Where() {
                return Or(RelRestriction("is a", "motion",  "relTypeSubsumptionKey", "destinationSubsumptionKey"));
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

    @Test(groups = "QueryServiceTests")
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

    @Test(groups = "QueryServiceTests")
    public void testAnd() throws IOException, Exception {
        AndTest andTest = new AndTest();
        NativeIdSetBI results = andTest.computeQuery();
        LOGGER.log(Level.INFO, "And query test results: {0}", results.size());
        assertEquals(REPORTS.getQueryCount("AndTest"), results.size());
    }

    @Test(groups = "QueryServiceTests")
    public void isChildOfTest() throws IOException, Exception {
        IsChildOfTest isChildOfTest = new IsChildOfTest();
        Query q3 = isChildOfTest.getQuery();
        q3.getViewCoordinate().setAllowedStatus(EnumSet.of(Status.ACTIVE));
        NativeIdSetBI results3 = q3.compute();
        LOGGER.log(Level.INFO, "Query result count {0}", results3.size());
        //assertEquals(REPORTS.getQueryCount("ConceptIsChildOfTest"), results3.size());
        assertEquals(REPORTS.getQueryCount("ConceptIsChildOfTest"), 21);
    }

    @Test(groups = "QueryServiceTests")
    public void isDescendentOfTest() throws IOException, Exception {
        IsDescendentOfTest isDescendent = new IsDescendentOfTest();
        Query q4 = isDescendent.getQuery();
        NativeIdSetBI results4 = q4.compute();
        LOGGER.log(Level.INFO, "ConceptIsDescendentOf query result count {0}", results4.size());
        assertEquals(REPORTS.getQueryCount("ConceptIsDescendentOfTest"), results4.size());
    }

    @Test(groups = "QueryServiceTests")
    public void isKindOfTest() throws IOException, Exception {
        IsKindOfTest kindOf = new IsKindOfTest();
        Query kindOfQuery = kindOf.getQuery();
        NativeIdSetBI kindOfResults = kindOfQuery.compute();
        NativeIdSetItrBI iter = kindOfResults.getSetBitIterator();
        while (iter.next()) {
            assertTrue(PersistentStore.get().isKindOf(iter.nid(), 
                    Snomed.PHYSICAL_FORCE.getNid(), VC_LATEST_ACTIVE_ONLY), "Testing: " +
                            Ts.get().getConcept(iter.nid()));
        }

        assertEquals(REPORTS.getQueryCount("IsKindOfTest"), kindOfResults.size());
    }

    @Test(groups = "QueryServiceTests")
    public void queryTest() throws IOException, Exception {
        Query q = new Query(VC_LATEST_ACTIVE_AND_INACTIVE) {
            @Override
            protected ForSetSpecification ForSetSpecification() throws IOException {
                ForSetSpecification forSetSpecification = new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
                return forSetSpecification;
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
                        And(Or(RelRestriction("is a", "motion"),
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
            protected ForSetSpecification ForSetSpecification() throws IOException {
                ForSetSpecification forSetSpecification = new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
                return forSetSpecification;
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
        
         ConceptSequenceSet activeSet = new ConceptSequenceSet();
        
        Ts.get().getConceptStream().forEach((ConceptChronicleBI cc) -> {
            for (ConceptVersionBI cv: cc.getVersions(q.getViewCoordinate())) {
                try {
                    if (cv.isActive()) {
                        activeSet.add(cc.getNid());
                    }
                } catch (IOException ex) {
                   throw new RuntimeException(ex);
                }
            }
        });


        NativeIdSetBI results = q.compute();

        LOGGER.log(Level.INFO, "Not test result size: {0}", results.size());
        assertEquals(activeSet.size() -1, results.size());
        //assertEquals(REPORTS.getQueryCount("NotTest") + cementSize, results.size());
    }

    @Test(groups = "QueryServiceTests")
    public void conceptForComponentTest() throws IOException, Exception {
        LOGGER.log(Level.INFO, "ConceptForComponentTest");
        ConceptForComponentTest cfcTest = new ConceptForComponentTest();
        NativeIdSetBI results = cfcTest.computeQuery();
        assertEquals(REPORTS.getQueryCount("ConceptForComponentTest"), results.size());
    }

    @Test(enabled = false)
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

    @Test(enabled = false)
    public void refsetContainsConceptTest() throws IOException, Exception {
        LOGGER.log(Level.INFO, "RefsetContainsConcept test");
        TermstoreChanges tc = new TermstoreChanges(VC_LATEST_ACTIVE_ONLY);
        tc.addRefsetMember();
        RefsetContainsConceptTest rccTest = new RefsetContainsConceptTest();
        NativeIdSetBI ids = rccTest.computeQuery();
        assertEquals(1, ids.size());

    }

    @Test(enabled = false)
    public void refsetContainsStringTest() throws Exception {
        LOGGER.log(Level.INFO, "RefsetContainsString test");
        TermstoreChanges tc = new TermstoreChanges(VC_LATEST_ACTIVE_ONLY);
        tc.addRefsetMember();
        RefsetContainsStringTest rcsTest = new RefsetContainsStringTest();
        NativeIdSetBI ids = rcsTest.computeQuery();
        assertEquals(1, ids.size());
    }

    @Test(enabled = false)
    public void refsetContainsKindOfConceptTest() throws Exception {
        LOGGER.log(Level.INFO, "RefsetContainsKindOfConcept test");
        TermstoreChanges tc = new TermstoreChanges(VC_LATEST_ACTIVE_ONLY);
        tc.addRefsetMember();
        RefsetContainsKindOfConceptTest rckocTest = new RefsetContainsKindOfConceptTest();
        NativeIdSetBI nids = rckocTest.computeQuery();
        assertEquals(1, nids.size());
    }

    @Test(groups = "QueryServiceTests")
    public void testOr2() throws IOException, Exception {
        LOGGER.log(Level.INFO, "Or test");
        OrTest orTest = new OrTest();
        NativeIdSetBI results = orTest.computeQuery();
        assertEquals(REPORTS.getQueryCount("OrTest2"), results.size());
    }

    @Test(groups = "QueryServiceTests")
    public void notTest2() throws IOException, Exception {
        LOGGER.log(Level.INFO, "Not test2");
        NotTest notTest = new NotTest();
        NativeIdSetBI results = notTest.computeQuery();
        assertEquals(REPORTS.getQueryCount("NotTest2"), results.size());
    }

    @Test(groups = "QueryServiceTests")
    public void ExampleTest() throws Exception {
        LOGGER.log(Level.INFO, "Example test");
        QueryExample test = new QueryExample();

        assertEquals(REPORTS.getQueryCount("ExampleQueryTest"), test.getResults().size());
    }

    @Test(enabled = false)
    public void descriptionActiveLuceneMatchTest() throws Exception {
        LOGGER.log(Level.INFO, "DescriptionActiveLuceneMatch test");
        Query q1 = new Query(VC_LATEST_ACTIVE_AND_INACTIVE) {
            @Override
            protected ForSetSpecification ForSetSpecification() throws IOException {
                ForSetSpecification forSetSpecification = new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
                return forSetSpecification;
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

        for (DescriptionVersionBI desc : PersistentStore.get().getConceptVersion(VC_LATEST_ACTIVE_AND_INACTIVE, Snomed.BARANYS_SIGN.getNid()).getDescriptionsActive()) {
            tc.setActiveStatus(desc, Status.INACTIVE);
        }
        DescriptionActiveLuceneMatchTest test = new DescriptionActiveLuceneMatchTest();
        NativeIdSetBI results2 = test.computeQuery();
        assertEquals(results2.size(), results1.size() - 1);
        for (DescriptionChronicleBI desc : PersistentStore.get().getConceptVersion(VC_LATEST_ACTIVE_AND_INACTIVE, Snomed.BARANYS_SIGN.getNid()).getDescriptions()) {
            Optional<? extends DescriptionVersionBI> descVersion = desc.getVersion(VC_LATEST_ACTIVE_AND_INACTIVE);
            tc.setActiveStatus(descVersion.get(), Status.ACTIVE);
        }
    }

    @Test(groups = "QueryServiceTests")
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

    @Test(enabled = false)
    public void DescriptionActiveRegexMatchTest() throws IOException, ContradictionException, InvalidCAB, Exception {
        LOGGER.log(Level.INFO, "Description active regex match test");
        TermstoreChanges tc = new TermstoreChanges(VC_LATEST_ACTIVE_AND_INACTIVE);
        for (DescriptionVersionBI desc : PersistentStore.get().getConceptVersion(VC_LATEST_ACTIVE_AND_INACTIVE, Snomed.ACCELERATION.getNid()).getDescriptionsActive()) {
            tc.setActiveStatus(desc, Status.INACTIVE);
        }
        DescriptionActiveRegexMatchTest test = new DescriptionActiveRegexMatchTest();
        try {
            NativeIdSetBI computeQuery = test.computeQuery();
            NativeIdSetItrBI iter = computeQuery.getSetBitIterator();
            while (iter.next()) {
                LOGGER.log(Level.INFO, "DescriptionActiveRegexMatch test: {0}", PersistentStore.get().getComponentVersion(VC_LATEST_ACTIVE_ONLY, iter.nid()));
            }
            //Since the reports doesn't account for the fact that descriptions for "Accleration" have been made inactive within the tests,
            //the result set from the report will still include "Acceleration"
            int expectedCardinality = REPORTS.getQueryCount("DescriptionActiveRegexMatchTest") - 1;
            LOGGER.log(Level.INFO, "DescriptionActiveRegexMAtch test size: {0}", computeQuery.size());
            assertEquals(expectedCardinality, computeQuery.size());
        } catch (Exception ex) {
            LOGGER.log(Level.INFO, null, ex);
            fail("DescriptionActiveRegexMatchTest failed and threw: " + ex.toString());
        } finally {
            for (DescriptionChronicleBI desc : PersistentStore.get().getConceptVersion(VC_LATEST_ACTIVE_AND_INACTIVE, Snomed.ACCELERATION.getNid()).getDescriptions()) {
                Optional<? extends DescriptionVersionBI> descVersion = desc.getVersion(VC_LATEST_ACTIVE_AND_INACTIVE);
                tc.setActiveStatus(descVersion.get(), Status.ACTIVE);
            }
        }
    }

    @Test(groups = "QueryServiceTests")
    public void testKindOfDiseaseVersioned() throws Exception {
        final SetViewCoordinate setVC = new SetViewCoordinate(2002, 1, 31, 0, 0);
        Query q = new Query(setVC.getViewCoordinate()) {
            @Override
            protected ForSetSpecification ForSetSpecification() throws IOException {
                ForSetSpecification forSetSpecification = new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
                return forSetSpecification;
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
        int exp = REPORTS.getQueryCount("ConceptIsKindOf('disease', 'v2')");
        assertEquals(exp, results.size());
    }

//    @Test(groups = "QueryServiceTests")
//    public void testConceptIsKindOfAcuteAllergicReactionVersioned() throws Exception {
//        LOGGER.log(Level.INFO, "ConceptIsKindOf Acute allergic reaction");
//        final SetViewCoordinate setVC = new SetViewCoordinate(2008, 1, 31, 0, 0);
//        Query q = new Query(setVC.getViewCoordinate()) {
//
//            @Override
//            protected NativeIdSetBI For() throws IOException {
//                return PersistentStore.get().getAllConceptNids();
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
//    @Test(groups = "QueryServiceTests")
//    public void testKindOfVenomInducedAnaphylaxis() throws IOException, Exception {
//        final SetViewCoordinate setVC = new SetViewCoordinate(2008, 1, 31, 0, 0);
//        Query q = new Query(setVC.getViewCoordinate()) {
//
//            @Override
//            protected NativeIdSetBI For() throws IOException {
//                return PersistentStore.get().getAllConceptNids();
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
//            LOGGER.log(Level.INFO, "OTF kind of Venom-induced anaphylaxis: {0}", PersistentStore.get().getComponent(iter.nid()));
//        }
//
//    }
}
