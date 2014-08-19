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
package org.ihtsdo.otf.query.integration.tests.rest;

import com.informatics.bdb.junit.ext.BdbTestRunner;
import com.informatics.bdb.junit.ext.BdbTestRunnerConfig;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import static java.lang.String.valueOf;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import org.apache.lucene.queryparser.classic.ParseException;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.ihtsdo.otf.query.implementation.Clause;
import org.ihtsdo.otf.query.implementation.ClauseSemantic;
import org.ihtsdo.otf.query.implementation.ForCollection;
import org.ihtsdo.otf.query.implementation.JaxbForQuery;
import org.ihtsdo.otf.query.implementation.LetMap;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.query.implementation.ReturnTypes;
import org.ihtsdo.otf.query.implementation.Where;
import org.ihtsdo.otf.query.implementation.WhereClause;
import org.ihtsdo.otf.query.implementation.versioning.StandardViewCoordinates;
import org.ihtsdo.otf.query.integration.tests.ConceptForComponentTest;
import org.ihtsdo.otf.query.integration.tests.ConceptIsTest;
import org.ihtsdo.otf.query.integration.tests.DescriptionActiveLuceneMatchTest;
import org.ihtsdo.otf.query.integration.tests.DescriptionActiveRegexMatchTest;
import org.ihtsdo.otf.query.integration.tests.ExampleQuery;
import org.ihtsdo.otf.query.integration.tests.FullySpecifiedNameForConceptTest;
import org.ihtsdo.otf.query.integration.tests.IsChildOfTest;
import org.ihtsdo.otf.query.integration.tests.IsDescendentOfTest;
import org.ihtsdo.otf.query.integration.tests.IsKindOfTest;
import org.ihtsdo.otf.query.integration.tests.JSONToReport;
import org.ihtsdo.otf.query.integration.tests.NotTest;
import org.ihtsdo.otf.query.integration.tests.OrTest;
import org.ihtsdo.otf.query.integration.tests.PreferredNameForConceptTest;
import org.ihtsdo.otf.query.integration.tests.RefsetContainsConceptTest;
import org.ihtsdo.otf.query.integration.tests.RefsetContainsKindOfConceptTest;
import org.ihtsdo.otf.query.integration.tests.RefsetContainsStringTest;
import org.ihtsdo.otf.query.integration.tests.RefsetLuceneMatchTest;
import org.ihtsdo.otf.query.integration.tests.RelRestriction2Test;
import org.ihtsdo.otf.query.integration.tests.RelRestrictionTest;
import org.ihtsdo.otf.query.integration.tests.RelTypeTest;
import org.ihtsdo.otf.query.integration.tests.SetViewCoordinate;
import org.ihtsdo.otf.query.integration.tests.XorTest;
import org.ihtsdo.otf.query.rest.server.LuceneResource;
import org.ihtsdo.otf.query.rest.server.QueryApplicationException;
import org.ihtsdo.otf.query.rest.server.QueryExceptionMapper;
import org.ihtsdo.otf.query.rest.server.QueryResource;
import org.ihtsdo.otf.tcc.api.blueprint.InvalidCAB;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.description.DescriptionChronicleBI;
import org.ihtsdo.otf.tcc.api.description.DescriptionVersionBI;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;
import org.ihtsdo.otf.tcc.api.nid.ConcurrentBitSet;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetItrBI;
import org.ihtsdo.otf.tcc.api.store.Ts;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 *
 * @author kec
 */
@RunWith(BdbTestRunner.class)
@BdbTestRunnerConfig()
public class RestQueryTest extends JerseyTest {

    private static final Logger LOGGER = Logger.getLogger(RestQueryTest.class.getName());
    private static final JSONToReport REPORTS = new JSONToReport(System.getProperty("user.dir") + "/target/test-resources/reports.json");

    public RestQueryTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        REPORTS.parseFile();
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    @Override
    public void setUp() {
    }

    @After
    @Override
    public void tearDown() {
    }

    @Override
    protected Application configure() {
        return new ResourceConfig(QueryResource.class, QueryExceptionMapper.class);
    }

    @Test
    public void nullParamsTest() {
        String resultString = target("query").
                request(MediaType.TEXT_PLAIN).get(String.class);
        assertEquals("Enter the required LET and WHERE parameters. See the documentation at "
                + "http://ihtsdo.github.io/OTF-Query-Services/query-documentation/docbook/query-documentation.html for more information.", resultString);
    }

    @Test
    public void testQuery() {
        LOGGER.log(Level.INFO, "ExampleQuery test");
        try {
            ExampleQuery q = new ExampleQuery(null);

            JAXBContext ctx = JaxbForQuery.get();

            String viewCoordinateXml = getXmlString(ctx,
                    StandardViewCoordinates.getSnomedInferredLatestActiveOnly());

            String forXml = getXmlString(ctx, new ForCollection());

            q.Let();
            Map<String, Object> map = q.getLetDeclarations();
            LetMap wrappedMap = new LetMap(map);
            String letMapXml = getXmlString(ctx, wrappedMap);

            WhereClause where = q.Where().getWhereClause();

            String whereXml = getXmlString(ctx, where);

            final String resultString = target("query").
                    queryParam("VIEWPOINT", viewCoordinateXml).
                    queryParam("FOR", forXml).
                    queryParam("LET", letMapXml).
                    queryParam("WHERE", whereXml).
                    queryParam("RETURN", ReturnTypes.DESCRIPTION_VERSION_FSN.name()).
                    request(MediaType.TEXT_PLAIN).get(String.class);

            LOGGER.log(Level.INFO, resultString);

            LOGGER.log(Level.INFO,
                    "Result: {0}", resultString);

            NativeIdSetBI nidSet = this.getNidSet(resultString);
            int exampleQueryCount = REPORTS.getQueryCount("ExampleQueryTest");
            assertEquals(exampleQueryCount, nidSet.size());

        } catch (JAXBException | IOException ex) {
            Logger.getLogger(RestQueryTest.class.getName()).log(Level.SEVERE, null, ex);
            fail(ex.toString());
        }
    }

    @Ignore
    @Test(expected = QueryApplicationException.class)
    public void malformedLetTest() throws UnsupportedEncodingException, QueryApplicationException {
        String viewPoint = "";
        String forSet = "";
        String letMap = "%3C%3Fxml+version%3D%221.0%22+encoding%3D%22UTF-8%22+standalone%3D%22yes%22%3F%3E%3Cns2%3AletMap+xmlns%3Ans2%3D%22http%3A%2F%2Fquery.jaxb.otf.ihtsdo.org%22+xmlns%3Ans4%3D%22http%3A%2F%2Fdisplay.object.jaxb.otf.ihtsdo.org%22+xmlns%3Ans3%3D%22http%3A%2F%2Fapi.chronicle.jaxb.otf.ihtsdo.org%22%3E%3Cmap%3E%3Centry%3E%3Ckey%3Emotion%3C%2Fkey%3E%3Cvalue+xsi%3Atype%3D%22ns3%3AconceptSpec%22+xmlns%3Axsi%3D%22http%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema-instance%22%3E%3Cdescription%3EMoon+%28physical+force%29%3C%2Fdescription%3E%3CuuidStrs%3E45a8fde8-535d-3d2a-b76b-95ab67718b41%3C%2FuuidStrs%3E%3C%2Fvalue%3E%3C%2Fentry%3E%3C%2Fmap%3E%3C%2Fns2%3AletMap%3E";
        String where = "%3C%3Fxml+version%3D%221.0%22+encoding%3D%22UTF-8%22+standalone%3D%22yes%22%3F%3E%3Cns2%3Aclause+xmlns%3Ans2%3D%22http%3A%2F%2Fquery.jaxb.otf.ihtsdo.org%22+xmlns%3Ans4%3D%22http%3A%2F%2Fdisplay.object.jaxb.otf.ihtsdo.org%22+xmlns%3Ans3%3D%22http%3A%2F%2Fapi.chronicle.jaxb.otf.ihtsdo.org%22%3E%3Cchildren%3E%3CletKeys%3Emotion%3C%2FletKeys%3E%3CletKeys%3ECurrent+view+coordinate%3C%2FletKeys%3E%3CsemanticString%3ECONCEPT_IS%3C%2FsemanticString%3E%3C%2Fchildren%3E%3CsemanticString%3EOR%3C%2FsemanticString%3E%3C%2Fns2%3Aclause%3E";
        String resultString = "";
        try {
            resultString = target("query").
                    queryParam("VIEWPOINT", viewPoint).
                    queryParam("FOR", forSet).
                    queryParam("LET", URLDecoder.decode(letMap, "UTF-8")).
                    queryParam("WHERE", URLDecoder.decode(where, "UTF-8")).
                    queryParam("RETURN", ReturnTypes.NIDS.name()).request(MediaType.TEXT_PLAIN).get(String.class);
            LOGGER.log(Level.INFO, resultString);
        } catch (QueryApplicationException e) {
            LOGGER.log(Level.INFO, "class: {0}", e.getClass());
            LOGGER.log(Level.INFO, "toString: {0}", e.toString());
            LOGGER.log(Level.INFO, "getMessage: {0}", e.getMessage());
            LOGGER.log(Level.INFO, "getCause: {0}", e.getCause());
            assertTrue(e.getMessage().matches(".*Validation exception.*"));
        }
        //assertTrue(resultString.contains("ConceptSpec"));
    }

    @Test
    public void testLuceneRestQuery() throws IOException, ContradictionException, ParseException, JAXBException {
        LuceneResource lr = new LuceneResource();
        String queryText = "oligophrenia";
        String luceneResource = null;
        try {
            luceneResource = lr.doQuery(queryText);
        } catch (Exception ex) {
            Logger.getLogger(RestQueryTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        assertTrue(luceneResource.matches(".*[Oo]ligophrenia.*"));
    }

    @Test
    public void conceptForComponentTest() throws IOException, JAXBException {
        LOGGER.log(Level.INFO, "ConceptForComponentTest");
        ConceptForComponentTest test = new ConceptForComponentTest();
        String resultString = returnResultString(test.getQuery());
        assertEquals(REPORTS.getQueryCount("ConceptForComponentTest"), getNidSet(resultString).size());
    }

    @Test
    public void conceptIsTest() throws IOException, JAXBException {
        LOGGER.log(Level.INFO, "ConceptIsTest");
        ConceptIsTest test = new ConceptIsTest();
        String resultString = returnResultString(test.getQuery(), ReturnTypes.CONCEPT_VERSION);
        assertEquals(REPORTS.getQueryCount("ConceptIs(Motion)"), getNidSet(resultString).size());
        LOGGER.log(Level.INFO, resultString);
        assertTrue(resultString.matches(".*<componentNid>" + Snomed.MOTION.getNid() + "</componentNid>.*"));
    }

    @Test
    public void fsnTest() throws JAXBException, IOException, Exception {
        LOGGER.log(Level.INFO, "FSN test");
        FullySpecifiedNameForConceptTest fsnTest = new FullySpecifiedNameForConceptTest();
        String resultString = returnResultString(fsnTest.getQuery());
        LOGGER.log(Level.INFO, resultString);
        assertEquals(REPORTS.getQueryCount("FSNTest"), getNidSet(resultString).size());
    }

    @Test
    public void isChildOfTest() throws IOException, JAXBException {
        LOGGER.log(Level.INFO, "IsChildOf test");
        IsChildOfTest icoTest = new IsChildOfTest();
        String resultString = returnResultString(icoTest.getQuery());
        assertEquals(REPORTS.getQueryCount("ConceptIsChildOfTest"), getNidSet(resultString).size());
    }

    @Test
    public void IsDescendentOfTest() throws IOException, JAXBException {
        LOGGER.log(Level.INFO, "IsDescendentOf test");
        IsDescendentOfTest idoTest = new IsDescendentOfTest();
        String resultString = returnResultString(idoTest.getQuery());
        assertEquals(REPORTS.getQueryCount("ConceptIsDescendentOfTest"), getNidSet(resultString).size());
    }

    @Test
    public void IsKindOfTest() throws IOException, JAXBException {
        LOGGER.log(Level.INFO, "IsKindOf test");
        IsKindOfTest ikoTest = new IsKindOfTest();
        String resultString = returnResultString(ikoTest.getQuery());
        assertEquals(REPORTS.getQueryCount("IsKindOfTest"), getNidSet(resultString).size());
    }

    @Test
    public void NotTest() throws IOException, JAXBException {
        LOGGER.log(Level.INFO, "Not test");
        NotTest notTest = new NotTest();
        String resultString = returnResultString(notTest.getQuery());
        assertEquals(REPORTS.getQueryCount("NotTest2"), getNidSet(resultString).size());
    }

    @Test
    public void OrTest() throws IOException, JAXBException {
        LOGGER.log(Level.INFO, "Or test");
        OrTest orTest = new OrTest();
        String resultString = returnResultString(orTest.getQuery());
        assertEquals(REPORTS.getQueryCount("OrTest"), getNidSet(resultString).size());
    }

    @Test
    public void PreferredNameForConceptTest() throws IOException, JAXBException {
        LOGGER.log(Level.INFO, "PreferredNameForConcept test");
        PreferredNameForConceptTest test = new PreferredNameForConceptTest();
        String resultString = returnResultString(test.getQuery());
        assertEquals(REPORTS.getQueryCount("PreferredTermTest"), getNidSet(resultString).size());
    }

    @Test
    public void RefsetContainsConceptTest() throws IOException, JAXBException {
        LOGGER.log(Level.INFO, "RefsetContainsConcept test");
        TermstoreChanges tc = new TermstoreChanges(StandardViewCoordinates.getSnomedInferredLatestActiveOnly());
        tc.addRefsetMember();
        RefsetContainsConceptTest test = new RefsetContainsConceptTest();
        String resultString = returnResultString(test.getQuery());
        assertEquals(1, getNidSet(resultString).size());
    }

    @Test
    public void RefsetContainsKindOfConceptTest() throws JAXBException, IOException {
        LOGGER.log(Level.INFO, "RefsetContainsKindOfConcept test");
        TermstoreChanges tc = new TermstoreChanges(StandardViewCoordinates.getSnomedInferredLatestActiveOnly());
        tc.addRefsetMember();
        RefsetContainsKindOfConceptTest test = new RefsetContainsKindOfConceptTest();
        String resultString = returnResultString(test.getQuery());
        assertEquals(1, getNidSet(resultString).size());
    }

    @Test
    public void RefsetContainsStringTest() throws JAXBException, IOException {
        LOGGER.log(Level.INFO, "RefsetContainsString test");
        TermstoreChanges tc = new TermstoreChanges(StandardViewCoordinates.getSnomedInferredLatestActiveOnly());
        tc.addRefsetMember();
        RefsetContainsStringTest test = new RefsetContainsStringTest();
        String resultString = returnResultString(test.getQuery());
        assertEquals(1, getNidSet(resultString).size());
    }

    @Test
    public void RefsetLuceneMatchTest() throws JAXBException, IOException {
        LOGGER.log(Level.INFO, "RefsetLuceneMatch test");
        RefsetLuceneMatchTest test = new RefsetLuceneMatchTest();
        String resultString = returnResultString(test.getQuery());
        assertEquals(1, getNidSet(resultString).size());
    }

    @Test
    public void RelRestrictionTest() throws IOException, JAXBException {
        LOGGER.log(Level.INFO, "RelRestriction test");
        RelRestrictionTest test = new RelRestrictionTest();
        String resultString = returnResultString(test.getQuery());
        assertEquals(REPORTS.getQueryCount("RelRestriction test"), getNidSet(resultString).size());
    }

    @Test
    public void RelRestrictionSubFalseTest() throws Exception {
        LOGGER.log(Level.INFO, "RelRestriction subsumption false test");
        RelRestriction2Test test = new RelRestriction2Test();
        String resultsString = returnResultString(test.getQuery());
        assertEquals(REPORTS.getQueryCount("RelRestrictionTest2"), getNidSet(resultsString).size());
    }

    @Test
    public void RelTypeTest() throws JAXBException, IOException {
        LOGGER.log(Level.INFO, "RelType test");
        RelTypeTest test = new RelTypeTest();
        String resultString = returnResultString(test.getQuery());
        assertEquals(REPORTS.getQueryCount("RelType('Finding site', 'Structure of endocrine system')"), getNidSet(resultString).size());
    }

    @Test
    public void XorTest() throws IOException, JAXBException {
        LOGGER.log(Level.INFO, "Xor test");
        XorTest test = new XorTest();
        String resultString = returnResultString(test.getQuery());
        assertEquals(REPORTS.getQueryCount("XorTest"), getNidSet(resultString).size());
    }

    @Test
    public void DescriptionRegexMatchTest() throws IOException, JAXBException {
        LOGGER.log(Level.INFO, "DescriptionRegexMatch test");
        JAXBContext ctx = JaxbForQuery.get();

        ForCollection fc = new ForCollection();
        List<UUID> uuids = new ArrayList<>();
        uuids.add(Ts.get().getComponent(Snomed.ASSOCIATED_FINDING.getNid()).getPrimordialUuid());
        uuids.add(Ts.get().getComponent(Snomed.CLINICAL_FINDING.getNid()).getPrimordialUuid());
        fc.setCustomCollection(uuids);
        String forSet = getXmlString(ctx, fc);

        HashMap<String, Object> letMap = new HashMap<>();
        letMap.put("regex", "[Cc]linical finding.*");
        LetMap wrappedMap = new LetMap(letMap);
        String letMapXml = getXmlString(ctx, wrappedMap);

        // Set the where clause
        Where where = new Where();
        WhereClause clause = new WhereClause();
        clause.setSemanticString(ClauseSemantic.DESCRIPTION_REGEX_MATCH.name());
        clause.getLetKeys().add("regex");
        where.setRootClause(clause);

        WhereClause cForC = new WhereClause();
        cForC.setSemanticString(ClauseSemantic.CONCEPT_FOR_COMPONENT.name());
        cForC.getChildren().add(clause);
        where.setRootClause(cForC);

        String whereXml = getXmlString(ctx, where);

        final String resultString = target("query").
                queryParam("VIEWPOINT", "null").
                queryParam("FOR", forSet).
                queryParam("LET", letMapXml).
                queryParam("WHERE", whereXml).
                queryParam("RETURN", "NIDS").
                request(MediaType.TEXT_PLAIN).get(String.class);

        assertEquals(1, getNidSet(resultString).size());
    }

    @Test
    public void DescriptionActiveLuceneMatchTest() throws JAXBException, IOException, Exception {
        LOGGER.log(Level.INFO, "DescriptionActiveLuceneMatch test");
        Query q1 = new Query() {

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

        TermstoreChanges tc = new TermstoreChanges(StandardViewCoordinates.getSnomedInferredLatestActiveOnly());

        for (DescriptionVersionBI desc : Ts.get().getConceptVersion(StandardViewCoordinates.getSnomedInferredLatestActiveOnly(), Snomed.BARANYS_SIGN.getNid()).getDescriptionsActive()) {
            tc.setActiveStatus(desc, Status.INACTIVE);
        }
        DescriptionActiveLuceneMatchTest test = new DescriptionActiveLuceneMatchTest();
        String resultString = returnResultString(test.getQuery());
        for (DescriptionChronicleBI desc : Ts.get().getConceptVersion(StandardViewCoordinates.getSnomedInferredLatestActiveAndInactive(), Snomed.BARANYS_SIGN.getNid()).getDescriptions()) {
            DescriptionVersionBI descVersion = desc.getVersion(StandardViewCoordinates.getSnomedInferredLatestActiveAndInactive());
            tc.setActiveStatus(descVersion, Status.ACTIVE);
        }
        assertEquals(0, getNidSet(resultString).size());
    }

    @Test
    public void ChangedFromPreviousVersionTest() throws IOException, JAXBException, ContradictionException, InvalidCAB {
        LOGGER.log(Level.INFO, "ChangedFromPreviousVersion test");
        SetViewCoordinate svc = new SetViewCoordinate(2010, 1, 31, 0, 0);
        ViewCoordinate previousVC = svc.getViewCoordinate();
        TermstoreChanges tc = new TermstoreChanges(previousVC);

        JAXBContext ctx = JaxbForQuery.get();

        ForCollection fc = new ForCollection();
        List<UUID> uuids = new ArrayList<>();
        NativeIdSetBI cb = new ConcurrentBitSet();
        cb.add(Snomed.CLINICAL_FINDING.getNid());
        cb.or(Ts.get().isChildOfSet(Snomed.CLINICAL_FINDING.getNid(), previousVC));
        NativeIdSetItrBI iter = cb.getSetBitIterator();
        while (iter.next()) {
            uuids.add(Ts.get().getComponentVersion(previousVC, iter.nid()).getPrimordialUuid());
        }
        fc.setCustomCollection(uuids);
        String forSet = getXmlString(ctx, fc);

        HashMap<String, Object> letMap = new HashMap<>();
        letMap.put("v2", previousVC);
        LetMap wrappedMap = new LetMap(letMap);
        String letMapXml = getXmlString(ctx, wrappedMap);

        // Set the where clause
        Where where = new Where();
        WhereClause clause = new WhereClause();
        clause.setSemanticString(ClauseSemantic.CHANGED_FROM_PREVIOUS_VERSION.name());
        clause.getLetKeys().add("v2");
        where.setRootClause(clause);

        WhereClause or = new WhereClause();
        or.setSemanticString(ClauseSemantic.OR.name());
        or.getChildren().add(clause);
        where.setRootClause(or);

        String whereXml = getXmlString(ctx, where);

        tc.modifyDesc("Admin status", Snomed.ADMINISTRATIVE_STATUSES.getNid());

        final String resultString = target("query").
                queryParam("VIEWPOINT", "null").
                queryParam("FOR", forSet).
                queryParam("LET", letMapXml).
                queryParam("WHERE", whereXml).
                queryParam("RETURN", "NIDS").
                request(MediaType.TEXT_PLAIN).get(String.class);

        tc.modifyDesc("Administrative statuses", Snomed.ADMINISTRATIVE_STATUSES.getNid());

        NativeIdSetBI results = this.getNidSet(resultString);
        assertEquals(1, results.size());

    }

    @Ignore
    @Test
    public void RelRestrictionNullBooleansTest() throws JAXBException, UnsupportedEncodingException {
        LOGGER.log(Level.INFO, "RelRestriction null Booleans test");

        HashMap<String, Object> letMap = new HashMap<>();
        letMap.put("acceleration", Snomed.ACCELERATION);
        letMap.put("is a", Snomed.IS_A);
        letMap.put("motion", Snomed.MOTION);
        letMap.put("false", false);
        LetMap wrappedMap = new LetMap(letMap);

        // Set the where clause
        Where where = new Where();
        WhereClause relRestriction = new WhereClause();
        relRestriction.setSemanticString(ClauseSemantic.REL_RESTRICTION.name());
        relRestriction.getLetKeys().add("acceleration");
        relRestriction.getLetKeys().add("is a");
        relRestriction.getLetKeys().add("motion");
        relRestriction.getLetKeys().add("false");
        where.setRootClause(relRestriction);

        String url = getURLString(null, null, wrappedMap, where, ReturnTypes.DESCRIPTION_VERSION_FSN);

        LOGGER.log(Level.INFO, url);

        String resultString = target("query" + url).
                request(MediaType.TEXT_PLAIN).get(String.class);
        assertEquals("Enter the required LET and WHERE parameters. See the documentation at "
                + "http://ihtsdo.github.io/OTF-Query-Services/query-documentation/docbook/query-documentation.html for more information.", resultString);
    }

//    @Test
//    public void RelRestrictionNullBooleansTest2() {
//        String url = "query?VIEWPOINT=&FOR=&LET=%3C%3Fxml+version%3D%221.0%22+encoding%3D%22UTF-8%22+standalone%3D%22yes%22%3F%3E%3Cns2%3AletMap+xmlns%3Ans2%3D%22http%3A%2F%2Fquery.jaxb.otf.ihtsdo.org%22%3E%3Cmap%3E%3Centry%3E%3Ckey%3Eacceleration%3C%2Fkey%3E%3Cvalue+xsi%3Atype%3D%22ns4%3AsimpleConceptSpecification%22+xmlns%3Ans4%3D%22http%3A%2F%2Fapi.chronicle.jaxb.otf.ihtsdo.org%22+xmlns%3Axsi%3D%22http%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema-instance%22%3E%3Cdescription%3EAcceleration+%28physical+force%29%3C%2Fdescription%3E%3Cuuid%3E6ef49616-e2c7-3557-b7f1-456a2c5a5e54%3C%2Fuuid%3E%3C%2Fvalue%3E%3C%2Fentry%3E%3Centry%3E%3Ckey%3Eis+a%3C%2Fkey%3E%3Cvalue+xsi%3Atype%3D%22ns4%3AsimpleConceptSpecification%22+xmlns%3Ans4%3D%22http%3A%2F%2Fapi.chronicle.jaxb.otf.ihtsdo.org%22+xmlns%3Axsi%3D%22http%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema-instance%22%3E%3Cdescription%3EIs+a+%28attribute%29%3C%2Fdescription%3E%3Cuuid%3Ec93a30b9-ba77-3adb-a9b8-4589c9f8fb25%3C%2Fuuid%3E%3C%2Fvalue%3E%3C%2Fentry%3E%3Centry%3E%3Ckey%3Emotion%3C%2Fkey%3E%3Cvalue+xsi%3Atype%3D%22ns4%3AsimpleConceptSpecification%22+xmlns%3Ans4%3D%22http%3A%2F%2Fapi.chronicle.jaxb.otf.ihtsdo.org%22+xmlns%3Axsi%3D%22http%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema-instance%22%3E%3Cdescription%3EMotion+%28physical+force%29%3C%2Fdescription%3E%3Cuuid%3E45a8fde8-535d-3d2a-b76b-95ab67718b41%3C%2Fuuid%3E%3C%2Fvalue%3E%3C%2Fentry%3E%3Centry%3E%3Ckey%3Efalse%3C%2Fkey%3E%3Cvalue+xsi%3Atype%3D%22xs%3Aboolean%22+xmlns%3Axs%3D%22http%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema%22+xmlns%3Axsi%3D%22http%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema-instance%22%3Efalse%3C%2Fvalue%3E%3C%2Fentry%3E%3C%2Fmap%3E%3C%2Fns2%3AletMap%3E&WHERE=%3C%3Fxml+version%3D%221.0%22+encoding%3D%22UTF-8%22+standalone%3D%22yes%22%3F%3E%3Cns2%3Awhere+xmlns%3Ans2%3D%22http%3A%2F%2Fquery.jaxb.otf.ihtsdo.org%22%3E%3CrootClause%3E%3CletKeys%3Eacceleration%3C%2FletKeys%3E%3CletKeys%3Eis+a%3C%2FletKeys%3E%3CletKeys%3Emotion%3C%2FletKeys%3E%3CletKeys%3Efalse%3C%2FletKeys%3E%3CsemanticString%3EREL_RESTRICTION%3C%2FsemanticString%3E%3C%2FrootClause%3E%3C%2Fns2%3Awhere%3E&RETURN=NIDS";
//        String resultString = target(url).
//                request(MediaType.TEXT_PLAIN).get(String.class);
//        NativeIdSetBI results = getNidSet(resultString);
//        assertEquals(1, results.size());
//
//    }
    @Test
    public void RelRestrictionSubsumptionFalse() throws JAXBException, UnsupportedEncodingException {
        LOGGER.log(Level.INFO, "RelRestriction");

        //LET and WHERE objects constructed in org.ihtsdo.otf.query.rest.client.examples.RelRestrictionExample
        String letMapXml = URLDecoder.decode("%3C%3Fxml+version%3D%221.0%22+encoding%3D%22UTF-8%22+standalone%3D%22yes%22%3F%3E%3Cns2%3AletMap+xmlns%3Ans2%3D%22http%3A%2F%2Fquery.jaxb.otf.ihtsdo.org%22%3E%3Cmap%3E%3Centry%3E%3Ckey%3Eacceleration%3C%2Fkey%3E%3Cvalue+xsi%3Atype%3D%22ns4%3AsimpleConceptSpecification%22+xmlns%3Ans4%3D%22http%3A%2F%2Fapi.chronicle.jaxb.otf.ihtsdo.org%22+xmlns%3Axsi%3D%22http%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema-instance%22%3E%3Cdescription%3EAcceleration+%28physical+force%29%3C%2Fdescription%3E%3Cuuid%3E6ef49616-e2c7-3557-b7f1-456a2c5a5e54%3C%2Fuuid%3E%3C%2Fvalue%3E%3C%2Fentry%3E%3Centry%3E%3Ckey%3Eis+a%3C%2Fkey%3E%3Cvalue+xsi%3Atype%3D%22ns4%3AsimpleConceptSpecification%22+xmlns%3Ans4%3D%22http%3A%2F%2Fapi.chronicle.jaxb.otf.ihtsdo.org%22+xmlns%3Axsi%3D%22http%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema-instance%22%3E%3Cdescription%3EIs+a+%28attribute%29%3C%2Fdescription%3E%3Cuuid%3Ec93a30b9-ba77-3adb-a9b8-4589c9f8fb25%3C%2Fuuid%3E%3C%2Fvalue%3E%3C%2Fentry%3E%3Centry%3E%3Ckey%3Emotion%3C%2Fkey%3E%3Cvalue+xsi%3Atype%3D%22ns4%3AsimpleConceptSpecification%22+xmlns%3Ans4%3D%22http%3A%2F%2Fapi.chronicle.jaxb.otf.ihtsdo.org%22+xmlns%3Axsi%3D%22http%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema-instance%22%3E%3Cdescription%3EMotion+%28physical+force%29%3C%2Fdescription%3E%3Cuuid%3E45a8fde8-535d-3d2a-b76b-95ab67718b41%3C%2Fuuid%3E%3C%2Fvalue%3E%3C%2Fentry%3E%3Centry%3E%3Ckey%3Efalse%3C%2Fkey%3E%3Cvalue+xsi%3Atype%3D%22xs%3Aboolean%22+xmlns%3Axs%3D%22http%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema%22+xmlns%3Axsi%3D%22http%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema-instance%22%3Efalse%3C%2Fvalue%3E%3C%2Fentry%3E%3C%2Fmap%3E%3C%2Fns2%3AletMap%3E", "UTF-8");
        String whereXml = URLDecoder.decode("%3C%3Fxml+version%3D%221.0%22+encoding%3D%22UTF-8%22+standalone%3D%22yes%22%3F%3E%3Cns2%3Awhere+xmlns%3Ans2%3D%22http%3A%2F%2Fquery.jaxb.otf.ihtsdo.org%22%3E%3CrootClause%3E%3CletKeys%3Eacceleration%3C%2FletKeys%3E%3CletKeys%3Eis+a%3C%2FletKeys%3E%3CletKeys%3Emotion%3C%2FletKeys%3E%3CletKeys%3Efalse%3C%2FletKeys%3E%3CsemanticString%3EREL_RESTRICTION%3C%2FsemanticString%3E%3C%2FrootClause%3E%3C%2Fns2%3Awhere%3E", "UTF-8");

        final String resultString = target("query").
                queryParam("VIEWPOINT", "null").
                queryParam("FOR", "null").
                queryParam("LET", letMapXml).
                queryParam("WHERE", whereXml).
                queryParam("RETURN", "NIDS").
                request(MediaType.TEXT_PLAIN).get(String.class);

        NativeIdSetBI results = this.getNidSet(resultString);
        assertEquals(1, results.size());
    }

    @Test
    public void DescriptionActiveRegexMatchTest() throws IOException, ContradictionException, InvalidCAB, Exception {
        LOGGER.log(Level.INFO, "Description active regex match test");
        TermstoreChanges tc = new TermstoreChanges(StandardViewCoordinates.getSnomedInferredLatestActiveAndInactive());
        for (DescriptionVersionBI desc : Ts.get().getConceptVersion(StandardViewCoordinates.getSnomedInferredLatestActiveOnly(), Snomed.ACCELERATION.getNid()).getDescriptionsActive()) {
            tc.setActiveStatus(desc, Status.INACTIVE);
        }

        DescriptionActiveRegexMatchTest test = new DescriptionActiveRegexMatchTest();
        String results = this.returnResultString(test.getQuery());

        NativeIdSetBI resultSet = this.getNidSet(results);

        for (DescriptionChronicleBI desc : Ts.get().getConceptVersion(StandardViewCoordinates.getSnomedInferredLatestActiveAndInactive(), Snomed.ACCELERATION.getNid()).getDescriptions()) {
            DescriptionVersionBI descVersion = desc.getVersion(StandardViewCoordinates.getSnomedInferredLatestActiveAndInactive());
            tc.setActiveStatus(descVersion, Status.ACTIVE);
        }

        assertEquals(3, resultSet.size());
    }

    @Test
    public void definitionalStateTest() throws UnsupportedEncodingException {
        LOGGER.log(Level.INFO, "Definitional state test.");

        String letXml = URLDecoder.decode("%3C%3Fxml+version%3D%221.0%22+encoding%3D%22UTF-8%22+standalone%3D%22yes%22%3F%3E%3Cns2%3AletMap+xmlns%3Ans2%3D%22http%3A%2F%2Fquery.jaxb.otf.ihtsdo.org%22+xmlns%3Ans4%3D%22http%3A%2F%2Fdisplay.object.jaxb.otf.ihtsdo.org%22+xmlns%3Ans3%3D%22http%3A%2F%2Fapi.chronicle.jaxb.otf.ihtsdo.org%22%3E%3Cmap%3E%3Centry%3E%3Ckey%3Emotion%3C%2Fkey%3E%3Cvalue+xsi%3Atype%3D%22ns3%3AconceptSpec%22+xmlns%3Axsi%3D%22http%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema-instance%22%3E%3Cdescription%3EMotion+%28physical+force%29%3C%2Fdescription%3E%3CuuidStrs%3E45a8fde8-535d-3d2a-b76b-95ab67718b41%3C%2FuuidStrs%3E%3C%2Fvalue%3E%3C%2Fentry%3E%3C%2Fmap%3E%3C%2Fns2%3AletMap%3E", "UTF-8");
        String whereXml = URLDecoder.decode("%3C%3Fxml+version%3D%221.0%22+encoding%3D%22UTF-8%22+standalone%3D%22yes%22%3F%3E%3Cns2%3Aclause+xmlns%3Ans2%3D%22http%3A%2F%2Fquery.jaxb.otf.ihtsdo.org%22+xmlns%3Ans4%3D%22http%3A%2F%2Fdisplay.object.jaxb.otf.ihtsdo.org%22+xmlns%3Ans3%3D%22http%3A%2F%2Fapi.chronicle.jaxb.otf.ihtsdo.org%22%3E%3Cchildren%3E%3CletKeys%3Emotion%3C%2FletKeys%3E%3CletKeys%3ECurrent+view+coordinate%3C%2FletKeys%3E%3CsemanticString%3ECONCEPT_IS%3C%2FsemanticString%3E%3C%2Fchildren%3E%3CsemanticString%3EOR%3C%2FsemanticString%3E%3C%2Fns2%3Aclause%3E", "UTF-8");

        final String resultString = target("query").
                queryParam("VIEWPOINT", "null").
                queryParam("FOR", "null").
                queryParam("LET", letXml).
                queryParam("WHERE", whereXml).
                queryParam("RETURN", "DESCRIPTION_VERSION_FSN").
                request(MediaType.TEXT_PLAIN).get(String.class);

        assertTrue(stringMatchesDefinitionalStateRegex(resultString));

        final String resultStringConcept = target("query").
                queryParam("VIEWPOINT", "null").
                queryParam("FOR", "null").
                queryParam("LET", letXml).
                queryParam("WHERE", whereXml).
                queryParam("RETURN", "CONCEPT_VERSION").
                request(MediaType.TEXT_PLAIN).get(String.class);

        assertTrue(stringMatchesDefinitionalStateRegex(resultStringConcept));

    }

    public boolean stringMatchesDefinitionalStateRegex(String result) {

        StringTokenizer st = new StringTokenizer(result, "<");

        ArrayList<String> definitionalStates = new ArrayList<>();

        boolean matches = false;

        while (st.hasMoreTokens()) {
            String next = st.nextToken();
            if (next.matches("definitionalState>.*")) {
                definitionalStates.add(next);
            }
        }

        for (String s : definitionalStates) {
            if (s.matches("definitionalState>NECESSARY") || s.matches("definitionalState>NECESSARY_AND_SUFFICIENT") || s.matches("definitionalState>UNDETERMINED") || s.matches("definitionalState>NOT_A_DEFINED_COMPONENT")) {
                matches = true;
            }
        }

        return matches;

    }

    public String getURLString(ViewCoordinate vc, ForCollection forCollection, LetMap let, Where where, ReturnTypes rt) throws JAXBException, UnsupportedEncodingException {
        StringBuilder bi = new StringBuilder();
        bi.append("?VIEW=");
        JAXBContext ctx = JaxbForQuery.get();
        if (vc != null) {
            bi.append(URLEncoder.encode(getXmlString(ctx, vc), "UTF-8"));
        }

        bi.append("&FOR=");
        if (forCollection != null) {
            bi.append(URLEncoder.encode(getXmlString(ctx, forCollection), "UTF-8"));
        }

        bi.append("&LET=");
        bi.append(URLEncoder.encode(getXmlString(ctx, let), "UTF-8"));

        bi.append("&WHERE=");
        bi.append(URLEncoder.encode(getXmlString(ctx, where), "UTF-8"));

        bi.append("&RETURN=");
        bi.append(valueOf(rt));

        return (bi.toString());
    }

    public String returnResultString(Query q, ReturnTypes returnType) throws JAXBException, IOException {
        JAXBContext ctx = JaxbForQuery.get();
        String viewCoordinateXml = getXmlString(ctx,
                StandardViewCoordinates.getSnomedInferredLatestActiveAndInactive());

        String forXml = getXmlString(ctx, new ForCollection());

        q.Let();
        Map<String, Object> map = q.getLetDeclarations();
        LetMap wrappedMap = new LetMap(map);
        String letMapXml = getXmlString(ctx, wrappedMap);

        WhereClause where = q.Where().getWhereClause();

        String whereXml = getXmlString(ctx, where);

        //Print the url
        StringBuilder urlBuilder = new StringBuilder("{default-host}/otf/query-service/query?");

        urlBuilder.append("VIEWPOINT=").append(
                "&FOR=").append(URLEncoder.encode(forXml, "UTF-8")).append(
                        "&LET=").append(URLEncoder.encode(letMapXml, "UTF-8")).append(
                        "&WHERE=").append(URLEncoder.encode(whereXml, "UTF-8")).append(
                        "&RETURN=").append(valueOf(returnType));

        LOGGER.log(Level.INFO, "URL: {0}", urlBuilder.toString());

        final String resultString = target("query").
                queryParam("VIEWPOINT", viewCoordinateXml).
                queryParam("FOR", forXml).
                queryParam("LET", letMapXml).
                queryParam("WHERE", whereXml).
                queryParam("RETURN", returnType.name()).
                request(MediaType.TEXT_PLAIN).get(String.class);

        return resultString;
    }

    public String returnResultString(Query q) throws JAXBException, IOException {
        return returnResultString(q, ReturnTypes.NIDS);
    }

    private NativeIdSetBI getNidSet(String resultString) {
        StringTokenizer st = new StringTokenizer(resultString, "<>");
        NativeIdSetBI results = new ConcurrentBitSet();
        while (st.hasMoreElements()) {
            String nextToken = st.nextToken();
            if (nextToken.matches("-[0-9]*")) {
                results.add(Integer.parseInt(nextToken));
            }
        }
        return results;
    }

    private static String getXmlString(JAXBContext ctx, Object obj) throws JAXBException {
        StringWriter writer;
        writer = new StringWriter();
        ctx.createMarshaller().marshal(obj, writer);
        String letMapXml = writer.toString();
        return letMapXml;
    }
}
