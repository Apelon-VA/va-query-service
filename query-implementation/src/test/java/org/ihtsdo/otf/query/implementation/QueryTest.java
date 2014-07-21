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
import java.util.Collection;
import java.util.EnumSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.api.concept.ConceptVersionBI;
import org.ihtsdo.otf.tcc.api.coordinate.StandardViewCoordinates;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.description.DescriptionChronicleBI;
import org.ihtsdo.otf.tcc.api.description.DescriptionVersionBI;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetItrBI;
import org.ihtsdo.otf.tcc.api.relationship.RelationshipVersionBI;
import org.ihtsdo.otf.tcc.api.store.Ts;
import org.ihtsdo.otf.tcc.ddo.concept.component.description.DescriptionVersionDdo;
import org.ihtsdo.otf.tcc.junit.BdbTestRunner;
import org.ihtsdo.otf.tcc.junit.BdbTestRunnerConfig;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
public class QueryTest {

    private static final String DIR = System.getProperty("user.dir");
    private static final JSONToReport REPORTS = new JSONToReport(DIR + "/target/test-resources/reports.json");
    private static final Logger LOGGER = Logger.getLogger(QueryTest.class.getName());
    private static ViewCoordinate VC_LATEST_ACTIVE_AND_INACTIVE;
    private static ViewCoordinate VC_LATEST_ACTIVE_ONLY;

    public QueryTest() {
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

    @Ignore
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

    @Ignore
    @Test
    public void testRelType() throws IOException, Exception {

        Query q = new Query() {

            @Override
            protected NativeIdSetBI For() throws IOException {
                return Ts.get().getAllConceptNids();
            }

            @Override
            public void Let() throws IOException {
                let("Finding site", Snomed.FINDING_SITE);
                let("Endocrine", Snomed.STRUCTURE_OF_ENDOCRINE_SYSTEM);
            }

            @Override
            public Clause Where() {
                return Or(RelType("Finding site", "Endocrine"));
            }
        };
        NativeIdSetBI results = q.compute();
        System.out.println("Relationship test: " + results.size());
        assertEquals(REPORTS.getQueryCount("RelType('Finding site', 'Structure of endocrine system')"), results.size());
    }

    @Ignore
    @Test
    public void testQuery() throws Exception {
        Query q = new Query() {
            @Override
            protected NativeIdSetBI For() throws IOException {
                return Ts.get().isKindOfSet(Snomed.MOTION.getNid(), StandardViewCoordinates.getSnomedInferredLatestActiveOnly());
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
        System.out.println("Query test results: " + results.size());
        for (Object o : q.returnDisplayObjects(results, ReturnTypes.UUIDS)) {
            System.out.println(o);
        }
    }

    @Ignore
    @Test
    public void testQuery2() {
        try {
            Query q = new Query() {

                @Override
                protected NativeIdSetBI For() throws IOException {
                    return Ts.get().getAllConceptNids();
                }

                @Override
                public void Let() throws IOException {
                    let("Is a", Snomed.IS_A);
                    let("Motion", Snomed.MOTION);
                }

                @Override
                public Clause Where() {
                    return Or(RelType("Is a", "Motion"));
                }
            };

            NativeIdSetBI compute = q.compute();
            assertEquals(6, compute.size());
        } catch (Exception ex) {
            Logger.getLogger(QueryTest.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    @Ignore
    @Test
    public void testRelRestriction() {
        try {
            Query q = new Query(StandardViewCoordinates.getSnomedInferredLatestActiveOnly()) {
                @Override
                protected NativeIdSetBI For() throws IOException {
                    return Ts.get().getAllConceptNids();
                }

                @Override
                public void Let() throws IOException {
                    let("Procedure site", Snomed.PROCEDURE_SITE);
                    let("Eye structure", Snomed.EYE_STRUCTURE);
                    let("Laser surgery", Snomed.LASER_SURGERY);
                }

                @Override
                public Clause Where() {
                    return Or(RelRestriction("Laser surgery", "Procedure site", "Eye structure"));
                }
            };

            NativeIdSetBI results = q.compute();
            ConceptVersionBI laserSurgery = Ts.get().getConceptVersion(StandardViewCoordinates.getSnomedInferredLatestActiveOnly(), Snomed.LASER_SURGERY.getNid());
            NativeIdSetItrBI setBitIterator = results.getSetBitIterator();
            while (setBitIterator.next()) {
                ConceptChronicleBI concept = Ts.get().getConcept(setBitIterator.nid());
                ConceptVersionBI version = concept.getVersion(StandardViewCoordinates.getSnomedInferredLatestActiveOnly());
                assertTrue(version.isKindOf(laserSurgery));
                Collection<? extends RelationshipVersionBI> relationshipsOutgoingActive = version.getRelationshipsOutgoingActive();
                boolean found = false;
                for (RelationshipVersionBI rel : relationshipsOutgoingActive) {
                    int destinationId = rel.getDestinationNid();
                    int typeId = rel.getTypeNid();
                }
            }
            assertEquals(REPORTS.getQueryCount("RelRestriction test"), results.size());

        } catch (IOException ex) {
            Logger.getLogger(QueryTest.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(QueryTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Ignore
    @Test
    public void isKindOfTest() throws IOException, Exception {
        ViewCoordinate vc = StandardViewCoordinates.getSnomedInferredLatestActiveOnly();
        vc.setAllowedStatus(EnumSet.of(Status.ACTIVE));
        Query q = new Query(vc) {
            @Override
            protected NativeIdSetBI For() throws IOException {
                return Ts.get().getAllConceptNids();
            }

            @Override
            public void Let() throws IOException {
                let("Physical force", Snomed.PHYSICAL_FORCE);
            }

            @Override
            public Clause Where() {
                return And(ConceptIsKindOf("Physical force"));
            }
        };

        NativeIdSetBI results = q.compute();

        assertEquals(171, results.size());
    }

    @Ignore
    @Test
    public void isKindOfBodyStructure() throws Exception {
        Query q = new Query() {

            @Override
            protected NativeIdSetBI For() throws IOException {
                return Ts.get().getAllConceptNids();
            }

            @Override
            public void Let() throws IOException {
                let("body structure", Snomed.BODY_STRUCTURE);
            }

            @Override
            public Clause Where() {
                return And(ConceptIsKindOf("body structure"));
            }
        };

        NativeIdSetBI results = q.compute();
        assertEquals(30589, results.size());
    }

    @Ignore
    @Test
    public void fsnTest() throws Exception {
        Query q = new Query() {

            @Override
            protected NativeIdSetBI For() throws IOException {
                return Ts.get().getAllConceptNids();
            }

            @Override
            public void Let() throws IOException {
                let("physical force", Snomed.PHYSICAL_FORCE);
            }

            @Override
            public Clause Where() {
                return Or(PreferredNameForConcept(ConceptIs("physical force")));
            }
        };

        NativeIdSetBI results = q.compute();
        assertEquals(1, results.size());

        NativeIdSetItrBI iter = results.getSetBitIterator();
        while (iter.next()) {
            DescriptionVersionBI desc = (DescriptionVersionBI) Ts.get().getComponentVersion(q.getViewCoordinate(), iter.nid());
            assertEquals("Physical force", desc.getText());
        }
    }

    @Test
    public void testDescriptionActiveRegexMatch() throws Exception {

        TermstoreChanges tc = new TermstoreChanges(VC_LATEST_ACTIVE_AND_INACTIVE);
        for (DescriptionVersionBI desc : Ts.get().getConceptVersion(VC_LATEST_ACTIVE_AND_INACTIVE, Snomed.ACCELERATION.getNid()).getDescriptionsActive()) {
            tc.setActiveStatus(desc, Status.INACTIVE);
        }

        Query q = new Query(VC_LATEST_ACTIVE_ONLY) {

            @Override
            protected NativeIdSetBI For() throws IOException {
                return Ts.get().isKindOfSet(Snomed.MOTION.getNid(), StandardViewCoordinates.getSnomedInferredLatestActiveAndInactive());
            }

            @Override
            public void Let() throws IOException {
                let("regex", ".*tion.*");
            }

            @Override
            public Clause Where() {
                return ConceptForComponent(DescriptionActiveRegexMatch("regex"));
            }

        };
        try {
            NativeIdSetBI computeQuery = q.compute();
            assertEquals(REPORTS.getQueryCount("DescriptionActiveRegexMatchTest") - 1, computeQuery.size());
        } catch (Exception ex) {
            LOGGER.log(Level.INFO, "", ex);
        } finally {
            for (DescriptionChronicleBI desc : Ts.get().getConceptVersion(VC_LATEST_ACTIVE_AND_INACTIVE, Snomed.ACCELERATION.getNid()).getDescriptions()) {
                DescriptionVersionBI descVersion = desc.getVersion(VC_LATEST_ACTIVE_AND_INACTIVE);
                tc.setActiveStatus(descVersion, Status.ACTIVE);
            }
        }
    }

    @Test
    public void testDescriptionLuceneMatch() throws Exception {

        LOGGER.log(Level.INFO, "Description Lucene Match test");

        Query q = new Query() {
            @Override
            protected NativeIdSetBI For() throws IOException {
                return Ts.get().getAllConceptNids();
            }

            @Override
            public void Let() throws IOException {
                let("oligophrenia", "oligophrenia");
            }

            @Override
            public Clause Where() {
                return DescriptionLuceneMatch("oligophrenia");
            }
        };

        NativeIdSetBI results = q.compute();

        NativeIdSetItrBI iter = results.getSetBitIterator();
        while (iter.next()) {
            LOGGER.log(Level.INFO, "Description lucene match: {0}", Ts.get().getComponentVersion(VC_LATEST_ACTIVE_AND_INACTIVE, iter.nid()).toUserString());
        }

        assertEquals(REPORTS.getQueryCount("DescriptionLuceneMatch('Oligophrenia')"), results.size());
    }

    @Test
    public void testConceptIs() throws Exception {
        Query q = new Query() {

            @Override
            protected NativeIdSetBI For() throws IOException {
                return Ts.get().getAllConceptNids();
            }

            @Override
            public void Let() throws IOException {
                let("Motion", Snomed.MOTION);
            }

            @Override
            public Clause Where() {
                return Or(ConceptIs("Motion"));
            }
        };

        NativeIdSetBI results = q.compute();
        assertEquals(REPORTS.getQueryCount("ConceptIsVersionedTest"), results.size());
        for (Object o : q.returnDisplayObjects(results, ReturnTypes.DESCRIPTION_VERSION_PREFERRED)) {
            DescriptionVersionDdo ddo = (DescriptionVersionDdo) o;
            assertEquals("Motion", ddo.getText());
            assertTrue(ddo.getComponentNid() == Ts.get().getConceptVersion(q.getViewCoordinate(), Snomed.MOTION.getNid()).getPreferredDescription().getNid());
        }
    }

    @Test
    public void XorVersionTest() throws IOException, Exception {
        final SetViewCoordinate setViewCoordinate = new SetViewCoordinate(2002, 1, 31, 0, 0);
        setViewCoordinate.getViewCoordinate().setAllowedStatus(EnumSet.of(Status.ACTIVE));
        LOGGER.log(Level.INFO, "ViewCoordinate in XorVersionTest: {0}", setViewCoordinate.getViewCoordinate().toString());
        Query q = new Query(StandardViewCoordinates.getSnomedInferredLatestActiveOnly()) {
            @Override
            protected NativeIdSetBI For() throws IOException {
                return Ts.get().getAllConceptNids();
            }

            @Override
            public void Let() throws IOException {
                let("disease", Snomed.DISEASE);
                let("v2", setViewCoordinate.getViewCoordinate());
            }

            @Override
            public Clause Where() {
                return Xor(ConceptIsKindOf("disease"), ConceptIsKindOf("disease", "v2"));
            }
        };

        NativeIdSetBI results = q.compute();
        int queryCount = REPORTS.getQueryCount("Xor(ConceptIsKindOf('disease'), ConceptIsKindOf('disease', 'v2'))");
        assertEquals(queryCount, results.size());
    }

    @Test
    public void testKindOfDisease() throws Exception {
        Query q = new Query() {

            @Override
            protected NativeIdSetBI For() throws IOException {
                return Ts.get().getAllConceptNids();
            }

            @Override
            public void Let() throws IOException {
                let("disease", Snomed.DISEASE);
            }

            @Override
            public Clause Where() {
                return Or(ConceptIsKindOf("disease"));
            }
        };
        NativeIdSetBI results = q.compute();
        int queryCount = REPORTS.getQueryCount("ConceptIsKindOf('disease')");
        assertEquals(queryCount, results.size());
    }

    @Test
    public void testKindOfDiseaseVersioned() throws Exception {
        final SetViewCoordinate setVC = new SetViewCoordinate(2002, 1, 31, 0, 0);
        Query q = new Query() {

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
        
        NativeIdSetItrBI iter = results.getSetBitIterator();
        ConceptVersionBI disease = Ts.get().getConceptVersion(setVC.getViewCoordinate(), Snomed.DISEASE.getNid());
        ConceptVersionBI cv;
        while(iter.next()) { 
           cv = Ts.get().getConceptVersion(setVC.getViewCoordinate(), iter.nid());
           assertTrue(Ts.get().isKindOf(cv.getNid(), disease.getNid(), setVC.getViewCoordinate()));
        }
        
        int exp = REPORTS.getQueryCount("ConceptIsKindOf('disease', 'v2')");
        assertEquals(exp, results.size());
    }
    
    @Test
    public void testConceptIsKindOfVersioned4() throws IOException, Exception {
        SetViewCoordinate setVC = new SetViewCoordinate(2008, 1, 31, 0, 0);
        final ViewCoordinate v2 = setVC.getViewCoordinate();
        Query q1 = new Query() {

            @Override
            protected NativeIdSetBI For() throws IOException {
                return Ts.get().getAllConceptNids();
            }

            @Override
            public void Let() throws IOException {
                let("v2", v2);
                let("acute", Snomed.ACUTE_ALLERGIC_REACTION);
            }

            @Override
            public Clause Where() {
                return And(ConceptIsKindOf("acute"), AndNot(ConceptIsKindOf("acute", "v2")));
            }
        };
        NativeIdSetBI results = q1.compute();
        assertTrue(!results.isEmpty());
        LOGGER.log(Level.INFO, "{0}", results.size());
    }   
    
}
