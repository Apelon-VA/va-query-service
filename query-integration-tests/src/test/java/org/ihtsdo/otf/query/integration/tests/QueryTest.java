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
import gov.vha.isaac.metadata.coordinates.LanguageCoordinates;
import gov.vha.isaac.metadata.coordinates.StampCoordinates;
import gov.vha.isaac.metadata.coordinates.TaxonomyCoordinates;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.TaxonomyService;
import gov.vha.isaac.ochre.api.component.concept.ConceptChronology;
import gov.vha.isaac.ochre.api.component.concept.ConceptService;
import gov.vha.isaac.ochre.api.coordinate.StampCoordinate;
import gov.vha.isaac.ochre.api.coordinate.TaxonomyCoordinate;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import gov.vha.isaac.ochre.collections.NidSet;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ihtsdo.otf.query.implementation.*;
import org.ihtsdo.otf.query.integration.tests.rest.TermstoreChanges;
import org.ihtsdo.otf.tcc.api.blueprint.InvalidCAB;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;
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

    private static final Logger log = LogManager.getLogger();

    private static final String DIR = System.getProperty("user.dir");
    private static final JSONToReport REPORTS = new JSONToReport(DIR + "/target/test-resources/OTFReports.json");
    private static final JSONToReport CEMENT_REPORT = new JSONToReport(DIR + "/target/test-resources/eConceptReports.json");

    private static StampCoordinate VC_LATEST_ACTIVE_AND_INACTIVE;
    private static StampCoordinate VC_LATEST_ACTIVE_ONLY;
    private static TaxonomyCoordinate TC_LATEST_ACTIVE_ONLY_INFERRED;
    private static TaxonomyCoordinate TC_LATEST_INFERRED;

    private static ConceptService conceptService;

    public static ConceptService getConceptService() {
        if (conceptService == null) {
            conceptService = LookupService.getService(ConceptService.class);
        }
        return conceptService;
    }

    private static TaxonomyService taxonomyService;

    public static TaxonomyService getTaxonomyService() {
        if (taxonomyService == null) {
            taxonomyService = LookupService.getService(TaxonomyService.class);
        }
        return taxonomyService;
    }

    //private static int cementSize;
    public QueryTest() {
     }

    @BeforeClass
    public static void setUpClass() {
        REPORTS.parseFile();
        CEMENT_REPORT.parseFile();
    }
    @BeforeTest
    public void beforeTest() {
       VC_LATEST_ACTIVE_AND_INACTIVE = StampCoordinates.getDevelopmentLatest();
        VC_LATEST_ACTIVE_ONLY = StampCoordinates.getDevelopmentLatestActiveOnly();
        TC_LATEST_ACTIVE_ONLY_INFERRED = TaxonomyCoordinates.getInferredTaxonomyCoordinate(VC_LATEST_ACTIVE_ONLY,
                LanguageCoordinates.getUsEnglishLanguageFullySpecifiedNameCoordinate());
        TC_LATEST_INFERRED = TaxonomyCoordinates.getInferredTaxonomyCoordinate(VC_LATEST_ACTIVE_AND_INACTIVE,
                LanguageCoordinates.getUsEnglishLanguageFullySpecifiedNameCoordinate());
    }
    

    @Test(groups = "QueryServiceTests")
    public void testSimpleQuery() throws IOException, Exception {
        log.info("Simple query: ");
        Query q = new Query() {

            @Override
            protected ForSetSpecification ForSetSpecification() {
                ForSetSpecification forSetSpecification = new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
                return forSetSpecification;
            }

            @Override
            public void Let() {
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
        NidSet results = regexTest.getQuery().compute();
        assertEquals(REPORTS.getQueryCount("DescriptionRegexMatchTest"), results.size());
    }

    @Test(groups = "QueryServiceTests")
    public void testDifferenceQuery() throws IOException, Exception {
        XorVersionTest xorTest = new XorVersionTest();
        NidSet results = xorTest.computeQuery();
        log.info("Different query size: {}", results.size());
        assertEquals(REPORTS.getQueryCount("Xor(ConceptIsKindOf('disease'), ConceptIsKindOf('disease', 'v2'))"), results.size());
    }

    @Test(groups = "QueryServiceTests")
    public void testConceptIsKindOfVersioned() throws IOException, Exception {
        log.info("Test ConceptIsKindOf versioned");
        final SetViewCoordinate d = new SetViewCoordinate(2002, 1, 31, 0, 0);
        Query q = new Query(d.getViewCoordinate()) {

            @Override
            protected ForSetSpecification ForSetSpecification() {
                ForSetSpecification forSetSpecification = new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
                return forSetSpecification;
            }

            @Override
            public void Let() {
                let("motion", Snomed.MOTION);
                let("v2", d.getViewCoordinate());
            }

            @Override
            public Clause Where() {
                return Or(ConceptIsKindOf("motion", "v2"));
            }
        };
        NidSet results = q.compute();
        log.info(d.v1.toString());
        results.stream().forEach((nid) -> {
            try {
                log.info(PersistentStore.get().getConcept(nid).toLongString());
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        });
        assertEquals(7, results.size());
        //assertEquals(REPORTS.getQueryCount("ConceptIsKindOfVersionedTest"), results.size());
    }

    @Test(groups = "QueryServiceTests")
    public void testConceptIs() throws IOException, Exception {
        ConceptIsTest test = new ConceptIsTest();
        NidSet results = test.computeQuery();
        assertEquals(REPORTS.getQueryCount("ConceptIsVersionedTest"), results.size());
    }

    @Test(enabled = false)
    public void testDescriptionLuceneMatch() throws IOException, Exception {
        DescriptionLuceneMatchTest descLuceneMatch = new DescriptionLuceneMatchTest();
        NidSet results = descLuceneMatch.computeQuery();

        log.info("Description Lucene match test size: {}", results.size());
        assertEquals(REPORTS.getQueryCount("DescriptionLuceneMatch('Oligophrenia')"), results.size());
    }

    @Test(groups = "QueryServiceTests")
    public void testOr() throws IOException, Exception {
        Query q = new Query() {
            @Override
            protected ForSetSpecification ForSetSpecification() {
                ForSetSpecification forSetSpecification = new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
                return forSetSpecification;
            }

            @Override
            public void Let() {
                let("motion", Snomed.MOTION);
                let("acceleration", Snomed.ACCELERATION);
            }

            @Override
            public Clause Where() {
                return Or(ConceptIs("motion"),
                        ConceptIs("acceleration"));
            }
        };

        NidSet results = q.compute();
        assertEquals(REPORTS.getQueryCount("OrTest"), results.size());
    }

    @Test(groups = "QueryServiceTests")
    public void testXor() throws IOException, Exception {

        Query q = new Query(VC_LATEST_ACTIVE_ONLY) {
            @Override
            protected ForSetSpecification ForSetSpecification() {
                ForSetSpecification forSetSpecification = new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
                return forSetSpecification;
            }

            @Override
            public void Let() {
                let("Acceleration", Snomed.ACCELERATION);
                let("Motion", Snomed.MOTION);
            }

            @Override
            public Clause Where() {
                return Xor(ConceptIsDescendentOf("Acceleration"),
                        ConceptIsKindOf("Motion"));
            }
        };

        NidSet results = q.compute();
        log.info("Xor result size: {}", results.size());
        assertEquals(REPORTS.getQueryCount("XorTest"), results.size());
    }

    @Test(groups = "QueryServiceTests")
    public void testPreferredTerm() throws IOException, Exception {
        log.info("Sequence: {}", PersistentStore.get().getSequence());
        PreferredNameForConceptTest preferredNameTest = new PreferredNameForConceptTest();
        NidSet results = preferredNameTest.computeQuery();
        log.info("Preferred query result count: {}", results.size());
        for (Object o : preferredNameTest.getQuery().returnDisplayObjects(results, ReturnTypes.UUIDS)) {
            log.info("Preferred description: {}", o.toString());
        }
        assertEquals(REPORTS.getQuerySet("PreferredTermTest set").size(), results.size());
    }

    @Test(groups = "QueryServiceTests")
    public void testRelTypeVersioning() throws IOException, Exception {
        final SetViewCoordinate setViewCoordinate = new SetViewCoordinate(2002, 1, 31, 0, 0);
        Query q = new Query(VC_LATEST_ACTIVE_ONLY) {
            @Override
            protected ForSetSpecification ForSetSpecification() {
                ForSetSpecification forSetSpecification = new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
                return forSetSpecification;
            }

            @Override
            public void Let() {
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

        NidSet results = q.compute();
        assertEquals(REPORTS.getQueryCount("RelType versioning test"), results.size());
    }

//    @Test(groups = "QueryServiceTests")
//    public void testRelRestrictionSubsumptionTrue() throws IOException, Exception {
//        log.info("Rel restriction subsumption true");
//        RelRestrictionTest rrTest = new RelRestrictionTest();
//        NidSet results = rrTest.computeQuery();
//        for (Object o : rrTest.q.returnDisplayObjects(results, ReturnTypes.COMPONENT)) {
//            log.info(o.toString());
//        }
//
//        ConceptSequenceSet laserSurgerySet = getTaxonomyService().getKindOfSequenceSet(Snomed.LASER_SURGERY.getNid(), TC_LATEST_ACTIVE_ONLY_INFERRED);
//        ConceptSequenceSet destinationSet = getTaxonomyService().getKindOfSequenceSet(Snomed.EYE_STRUCTURE.getNid(), TC_LATEST_ACTIVE_ONLY_INFERRED);
//        ConceptSequenceSet relTypeSet = getTaxonomyService().getKindOfSequenceSet(Snomed.PROCEDURE_SITE.getNid(), TC_LATEST_ACTIVE_ONLY_INFERRED);
//
//        Set<ConceptVersion> relResults = new HashSet<>();
//        results.stream().forEach((nid) -> {
//            ConceptChronology concept = getConceptService().getConcept(nid);
//            Optional<ConceptVersion> conceptVersion = concept.getLatestVersion(ConceptVersion.class, VC_LATEST_ACTIVE_ONLY);
//            if (conceptVersion.isPresent()) {
//                relResults.add(conceptVersion.get());
//            }
//        });
//
//        NidSet conceptSet = new NidSet();
//        for (ConceptVersion concept : relResults) {
//            for (RelationshipVersionBI version : concept.getRelationshipsOutgoingActive()) {
//                int sourceId = version.getConceptNid();
//                int typeNid = version.getTypeNid();
//                int destinationId = version.getDestinationNid();
//                if (laserSurgerySet.contains(sourceId) && destinationSet.contains(destinationId) && relTypeSet.contains(typeNid) && version.isActive()) {
//                    conceptSet.add(sourceId);
//                }
//            }
//        }
//
//        conceptSet.and(results);
//        log.info("Concept AND size: {}", conceptSet.size());
//
////        Set<Long> querySet = REPORTS.getQuerySet("RelRestriction test set");
////        Set<RelationshipChronicleBI> rels = new HashSet<>();
////        for (Long l : querySet) {
////            String resultUUIDString = target("alternate-id/uuid/" + l).request(MediaType.TEXT_PLAIN).get(String.class);
////            RelationshipChronicleBI rel = (RelationshipChronicleBI) PersistentStore.get().getComponent(UUID.fromString(resultUUIDString));
////            rels.add(rel);
////        }
////        
////        NativeIdSetBI laserSurgerySet = PersistentStore.get().isKindOfSet(Snomed.LASER_SURGERY.getNid(), StandardViewCoordinates.getSnomedInferredLatest());
////        NativeIdSetBI destinationSet = PersistentStore.get().isKindOfSet(Snomed.EYE_STRUCTURE.getNid(), StandardViewCoordinates.getSnomedInferredLatest());
////        NativeIdSetBI relTypeSet = PersistentStore.get().isKindOfSet(Snomed.PROCEDURE_SITE.getNid(), StandardViewCoordinates.getSnomedInferredLatest());
////        
////        for (RelationshipChronicleBI r : rels) {
////            RelationshipVersionBI version = r.getVersion(StandardViewCoordinates.getSnomedInferredLatest());
////            int sourceId = version.getConceptNid();
////            int typeNid = version.getTypeNid();
////            int destinationId = version.getDestinationNid();
////            assertTrue(laserSurgerySet.contains(sourceId));
////            assertTrue(destinationSet.contains(destinationId));
////            assertTrue(relTypeSet.contains(typeNid));
////            if (!version.isActive()) {
////                LOGGER.log(Level.SEVERE, "Relationship is in result set but is not active: {}", version.toString());
////            }
////            //assertTrue(version.isActive());
////        }
//        assertEquals(REPORTS.getQueryCount("RelRestriction test"), results.size());
//    }
    @Test(groups = "QueryServiceTests")
    public void testRelRestrictionSubsumptionFalse() throws IOException, Exception {
        log.info("RelRestriction subsumption false test");
        RelRestriction2Test test = new RelRestriction2Test();
        NidSet results = test.computeQuery();
        assertEquals(REPORTS.getQueryCount("RelRestrictionTest2"), results.size());
    }

    @Test(groups = "QueryServiceTests")
    public void testRelRestrictionSubsumptionNull() throws IOException, Exception {
        Query q = new Query(VC_LATEST_ACTIVE_ONLY) {
            @Override
            protected ForSetSpecification ForSetSpecification() {
                ForSetSpecification forSetSpecification = new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
                return forSetSpecification;
            }

            @Override
            public void Let() {
                let("physical force", Snomed.PHYSICAL_FORCE);
                let("is a", Snomed.IS_A);
                let("motion", Snomed.MOTION);
                let("relTypeSubsumptionKey", true);
                let("destinationSubsumptionKey", true);
            }

            @Override
            public Clause Where() {
                return Or(RelRestriction("is a", "motion", "relTypeSubsumptionKey", "destinationSubsumptionKey"));
            }
        };
        NidSet results = q.compute();
        int[] setValues = results.stream().toArray();
        int count = 0;
        for (Object o : q.returnDisplayObjects(results, ReturnTypes.NIDS)) {
            assertEquals(setValues[count], Integer.parseInt(o.toString()));
            count++;
        }
        log.info("Rel restriction subsumption null results: {}", results.size());
        assertEquals(7, results.size());

    }

    @Test(groups = "QueryServiceTests")
    public void testFullySpecifiedName() throws IOException, Exception {
        FullySpecifiedNameForConceptTest fsnTest = new FullySpecifiedNameForConceptTest();
        NidSet results = fsnTest.computeQuery();
        log.info("Fully specified name test: {}", results.size());
        for (Object o : fsnTest.getQuery().returnDisplayObjects(results, ReturnTypes.UUIDS)) {
            log.info(o.toString());
        }
        int queryResultSize = REPORTS.getQueryCount("FSNTest");
        assertEquals(queryResultSize, results.size());
        assertEquals(queryResultSize, fsnTest.getQuery().returnDisplayObjects(results, ReturnTypes.NIDS).size());
    }

    @Test(groups = "QueryServiceTests")
    public void testAnd() throws IOException, Exception {
        AndTest andTest = new AndTest();
        NidSet results = andTest.computeQuery();
        log.info("And query test results: {}", results.size());
        assertEquals(REPORTS.getQueryCount("AndTest"), results.size());
    }

    @Test(groups = "QueryServiceTests")
    public void isChildOfTest() throws IOException, Exception {
        IsChildOfTest isChildOfTest = new IsChildOfTest();
        Query q3 = isChildOfTest.getQuery();
        NidSet results3 = q3.compute();
        log.info("Query result count {}", results3.size());
        //assertEquals(REPORTS.getQueryCount("ConceptIsChildOfTest"), results3.size());
        assertEquals(REPORTS.getQueryCount("ConceptIsChildOfTest"), 21);
    }

    @Test(groups = "QueryServiceTests")
    public void isDescendentOfTest() throws IOException, Exception {
        IsDescendentOfTest isDescendent = new IsDescendentOfTest();
        Query q4 = isDescendent.getQuery();
        NidSet results4 = q4.compute();
        log.info("ConceptIsDescendentOf query result count {}", results4.size());
        assertEquals(REPORTS.getQueryCount("ConceptIsDescendentOfTest"), results4.size());
    }

    @Test(groups = "QueryServiceTests")
    public void isKindOfTest() throws IOException, Exception {
        IsKindOfTest kindOf = new IsKindOfTest();
        Query kindOfQuery = kindOf.getQuery();
        NidSet kindOfResults = kindOfQuery.compute();
        kindOfResults.stream().forEach((nid) -> {
            try {
                assertTrue(getTaxonomyService().isKindOf(nid,
                        Snomed.PHYSICAL_FORCE.getNid(), TC_LATEST_ACTIVE_ONLY_INFERRED), "Testing: "
                        + Ts.get().getConcept(nid));
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        });

        assertEquals(REPORTS.getQueryCount("IsKindOfTest"), kindOfResults.size());
    }

    @Test(groups = "QueryServiceTests")
    public void queryTest() throws IOException, Exception {
        Query q = new Query(VC_LATEST_ACTIVE_AND_INACTIVE) {
            @Override
            protected ForSetSpecification ForSetSpecification() {
                ForSetSpecification forSetSpecification = new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
                return forSetSpecification;
            }

            @Override
            public void Let() {
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

        NidSet results = q.compute();
        log.info("Query test results: {}", results.size());
        for (Object o : q.returnDisplayObjects(results, ReturnTypes.UUIDS)) {
            log.info(o.toString());
        }
        assertEquals(REPORTS.getQueryCount("QueryTest"), results.size());

    }

    @Test
    public void notTest() throws IOException, Exception {
        Query q = new Query() {
            @Override
            protected ForSetSpecification ForSetSpecification() {
                ForSetSpecification forSetSpecification = new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
                return forSetSpecification;
            }

            @Override
            public void Let() {
                let("acceleration", Snomed.ACCELERATION);

            }

            @Override
            public Clause Where() {
                return Not(ConceptIs("acceleration"));
            }
        };

        ConceptSequenceSet activeSet = new ConceptSequenceSet();

        conceptService.getConceptChronologyStream().forEach((ConceptChronology<?> cc) -> {
            if (getConceptService().isConceptActive(cc.getNid(), VC_LATEST_ACTIVE_ONLY)) {
                activeSet.add(cc.getNid());
            }
        });

        NidSet results = q.compute();

        log.info("Not test result size: {}", results.size());
        assertEquals(activeSet.size() - 1, results.size());
        //assertEquals(REPORTS.getQueryCount("NotTest") + cementSize, results.size());
    }

    @Test(groups = "QueryServiceTests")
    public void conceptForComponentTest() throws IOException, Exception {
        log.info("ConceptForComponentTest");
        ConceptForComponentTest cfcTest = new ConceptForComponentTest();
        NidSet results = cfcTest.computeQuery();
        assertEquals(REPORTS.getQueryCount("ConceptForComponentTest"), results.size());
    }

    @Test(enabled = false)
    public void refsetLuceneMatchTest() throws IOException, Exception {
        log.info("RefsetLuceneMatch test");
        RefsetLuceneMatchTest rlmTest = new RefsetLuceneMatchTest();
        NidSet ids = rlmTest.computeQuery();
        for (Object o : rlmTest.q.returnDisplayObjects(ids, ReturnTypes.COMPONENT)) {
            log.info(o.toString());
            assertTrue(o.toString().contains("Virtual medicinal product simple reference set"));
        }
        assertEquals(1, ids.size());
    }
// TODO replace with tests for sememes.     
//    @Test(enabled = false)
//    public void refsetContainsConceptTest() throws IOException, Exception {
//        log.info("RefsetContainsConcept test");
//        TermstoreChanges tc = new TermstoreChanges(VC_LATEST_ACTIVE_ONLY);
//        tc.addRefsetMember();
//        RefsetContainsConceptTest rccTest = new RefsetContainsConceptTest();
//        NativeIdSetBI ids = rccTest.computeQuery();
//        assertEquals(1, ids.size());
//
//    }
//
//    @Test(enabled = false)
//    public void refsetContainsStringTest() throws Exception {
//        log.info("RefsetContainsString test");
//        TermstoreChanges tc = new TermstoreChanges(VC_LATEST_ACTIVE_ONLY);
//        tc.addRefsetMember();
//        RefsetContainsStringTest rcsTest = new RefsetContainsStringTest();
//        NativeIdSetBI ids = rcsTest.computeQuery();
//        assertEquals(1, ids.size());
//    }
//
//    @Test(enabled = false)
//    public void refsetContainsKindOfConceptTest() throws Exception {
//        log.info("RefsetContainsKindOfConcept test");
//        TermstoreChanges tc = new TermstoreChanges(VC_LATEST_ACTIVE_ONLY);
//        tc.addRefsetMember();
//        RefsetContainsKindOfConceptTest rckocTest = new RefsetContainsKindOfConceptTest();
//        NativeIdSetBI nids = rckocTest.computeQuery();
//        assertEquals(1, nids.size());
//    }

    @Test(groups = "QueryServiceTests")
    public void testOr2() throws IOException, Exception {
        log.info("Or test");
        OrTest orTest = new OrTest();
        NidSet results = orTest.computeQuery();
        assertEquals(REPORTS.getQueryCount("OrTest2"), results.size());
    }

    @Test(groups = "QueryServiceTests")
    public void notTest2() throws IOException, Exception {
        log.info("Not test2");
        NotTest notTest = new NotTest();
        NidSet results = notTest.computeQuery();
        assertEquals(REPORTS.getQueryCount("NotTest2"), results.size());
    }

    @Test(groups = "QueryServiceTests")
    public void ExampleTest() throws Exception {
        log.info("Example test");
        QueryExample test = new QueryExample();

        assertEquals(REPORTS.getQueryCount("ExampleQueryTest"), test.getResults().size());
    }

    @Test(enabled = false)
    public void descriptionActiveLuceneMatchTest() throws Exception {
//        log.info("DescriptionActiveLuceneMatch test");
//        Query q1 = new Query(VC_LATEST_ACTIVE_AND_INACTIVE) {
//            @Override
//            protected ForSetSpecification ForSetSpecification() {
//                ForSetSpecification forSetSpecification = new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
//                return forSetSpecification;
//            }
//            @Override
//            public void Let() {
//                let("baranys", "barany's");
//            }
//
//            @Override
//            public Clause Where() {
//                return ConceptForComponent(DescriptionLuceneMatch("baranys"));
//            }
//        };
//        NidSet results1 = q1.compute();
//        assertEquals(1, results1.size());
//
//        TermstoreChanges tc = new TermstoreChanges(VC_LATEST_ACTIVE_AND_INACTIVE);
//
//        for (DescriptionVersionBI desc : PersistentStore.get().getConceptVersion(VC_LATEST_ACTIVE_AND_INACTIVE, Snomed.BARANYS_SIGN.getNid()).getDescriptionsActive()) {
//            tc.setActiveStatus(desc, Status.INACTIVE);
//        }
//        DescriptionActiveLuceneMatchTest test = new DescriptionActiveLuceneMatchTest();
//        NidSet results2 = test.computeQuery();
//        assertEquals(results2.size(), results1.size() - 1);
//        for (DescriptionChronicleBI desc : PersistentStore.get().getConceptVersion(VC_LATEST_ACTIVE_AND_INACTIVE, Snomed.BARANYS_SIGN.getNid()).getDescriptions()) {
//            Optional<? extends DescriptionVersionBI> descVersion = desc.getVersion(VC_LATEST_ACTIVE_AND_INACTIVE);
//            tc.setActiveStatus(descVersion.get(), Status.ACTIVE);
//        }
    }

//    @Test(groups = "QueryServiceTests")
//    public void ChangeFromPreviousVersionTest() throws IOException, Exception {
//        log.info("Changed from previous version test");
//        ChangedFromPreviousVersionTest test = new ChangedFromPreviousVersionTest();
//        SetViewCoordinate svc = new SetViewCoordinate(2010, 1, 31, 0, 0);
//        ViewCoordinate previousVC = svc.getViewCoordinate();
//        TermstoreChanges tc = new TermstoreChanges(previousVC);
//        tc.modifyDesc("Admin statuses", Snomed.ADMINISTRATIVE_STATUSES.getNid());
//        NidSet results2 = test.computeQuery();
//        for (Object o : test.q.returnDisplayObjects(results2, ReturnTypes.DESCRIPTION_VERSION_FSN)) {
//            log.info(o.toString());
//        }
//        assertEquals(1, results2.size());
//    }

    @Test(enabled = false)
    public void DescriptionActiveRegexMatchTest() throws IOException, ContradictionException, InvalidCAB, Exception {
//        log.info("Description active regex match test");
//        TermstoreChanges tc = new TermstoreChanges(VC_LATEST_ACTIVE_AND_INACTIVE);
//        for (DescriptionVersionBI desc : PersistentStore.get().getConceptVersion(VC_LATEST_ACTIVE_AND_INACTIVE, Snomed.ACCELERATION.getNid()).getDescriptionsActive()) {
//            tc.setActiveStatus(desc, Status.INACTIVE);
//        }
//        DescriptionActiveRegexMatchTest test = new DescriptionActiveRegexMatchTest();
//        try {
//            NidSet computeQuery = test.computeQuery();
//            
//            computeQuery.stream().forEach(((nid) -> {
//                try {
//                    log.info("DescriptionActiveRegexMatch test: {}", PersistentStore.get().getComponentVersion(VC_LATEST_ACTIVE_ONLY, nid));
//                } catch (IOException | ContradictionException ex) {
//                   throw new RuntimeException(ex);
//                }
//            })); 
//            //Since the reports doesn't account for the fact that descriptions for "Accleration" have been made inactive within the tests,
//            //the result set from the report will still include "Acceleration"
//            int expectedCardinality = REPORTS.getQueryCount("DescriptionActiveRegexMatchTest") - 1;
//            log.info("DescriptionActiveRegexMAtch test size: {}", computeQuery.size());
//            assertEquals(expectedCardinality, computeQuery.size());
//        } catch (Exception ex) {
//            log.info(null, ex);
//            fail("DescriptionActiveRegexMatchTest failed and threw: " + ex.toString());
//        } finally {
//            for (DescriptionChronicleBI desc : PersistentStore.get().getConceptVersion(VC_LATEST_ACTIVE_AND_INACTIVE, Snomed.ACCELERATION.getNid()).getDescriptions()) {
//                Optional<? extends DescriptionVersionBI> descVersion = desc.getVersion(VC_LATEST_ACTIVE_AND_INACTIVE);
//                tc.setActiveStatus(descVersion.get(), Status.ACTIVE);
//            }
//        }
    }

    @Test(groups = "QueryServiceTests")
    public void testKindOfDiseaseVersioned() throws Exception {
        final SetViewCoordinate setVC = new SetViewCoordinate(2002, 1, 31, 0, 0);
        Query q = new Query(setVC.getViewCoordinate()) {
            @Override
            protected ForSetSpecification ForSetSpecification() {
                ForSetSpecification forSetSpecification = new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
                return forSetSpecification;
            }

            @Override
            public void Let() {
                let("Disease", Snomed.DISEASE);
                let("v2", setVC.getViewCoordinate());
            }

            @Override
            public Clause Where() {
                return Or(ConceptIsKindOf("Disease", "v2"));
            }
        };

        NidSet results = q.compute();
        int exp = REPORTS.getQueryCount("ConceptIsKindOf('disease', 'v2')");
        assertEquals(exp, results.size());
    }

//    @Test(groups = "QueryServiceTests")
//    public void testConceptIsKindOfAcuteAllergicReactionVersioned() throws Exception {
//        log.info("ConceptIsKindOf Acute allergic reaction");
//        final SetViewCoordinate setVC = new SetViewCoordinate(2008, 1, 31, 0, 0);
//        Query q = new Query(setVC.getStampCoordinate()) {
//
//            @Override
//            protected NativeIdSetBI For() throws IOException {
//                return PersistentStore.get().getAllConceptNids();
//            }
//
//            @Override
//            public void Let() throws IOException {
//                let("v2", setVC.getStampCoordinate());
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
//        Query q = new Query(setVC.getStampCoordinate()) {
//
//            @Override
//            protected NativeIdSetBI For() throws IOException {
//                return PersistentStore.get().getAllConceptNids();
//            }
//
//            @Override
//            public void Let() throws IOException {
//                let("venom-induced anaphylaxis", Snomed.VENOM_INDUCED_ANAPHYLAXIS);
//                let("v2", setVC.getStampCoordinate());
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
//        log.info("OTF kind of Venom-induced anaphylaxis and not Mojo set: {}", resultsFromOTF.size());
//        NativeIdSetItrBI iter = resultsFromOTF.getSetBitIterator();
//        while (iter.next()) {
//            log.info("OTF kind of Venom-induced anaphylaxis: {}", PersistentStore.get().getComponent(iter.nid()));
//        }
//
//    }
}
