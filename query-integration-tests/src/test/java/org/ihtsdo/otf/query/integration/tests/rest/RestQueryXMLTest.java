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
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import javax.ws.rs.core.Application;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.ihtsdo.otf.jaxb.chronicle.api.SimpleViewCoordinate;
import org.ihtsdo.otf.jaxb.query.ClauseSemantic;
import org.ihtsdo.otf.jaxb.query.ForCollection;
import org.ihtsdo.otf.jaxb.query.ForCollectionContents;
import org.ihtsdo.otf.jaxb.query.LetMap;
import org.ihtsdo.otf.jaxb.query.ReturnTypes;
import org.ihtsdo.otf.jaxb.query.Where;
import org.ihtsdo.otf.jaxb.query.WhereClause;
import org.ihtsdo.otf.query.implementation.JaxbForQuery;
import org.ihtsdo.otf.query.rest.server.QueryResource;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 *
 * @author dylangrald
 */
@RunWith(BdbTestRunner.class)
@BdbTestRunnerConfig()
public class RestQueryXMLTest extends JerseyTest {

    public RestQueryXMLTest() {
    }

    @BeforeClass
    public static void setUpClass() {
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
        return new ResourceConfig(QueryResource.class);
//        return new WebAppDescriptor.Builder()
//            	.initParam(WebComponent.RESOURCE_CONFIG_CLASS,
//                      ClassNamesResourceConfig.class.getName())
//                .initParam(
//                      ClassNamesResourceConfig.PROPERTY_CLASSNAMES,
//                      TodoResource.class.getName() + ";"
//                              + MockTodoServiceProvider.class.getName() + ";"
//                              + NotFoundMapper.class.getName()).build();
    }

    @Ignore
    @Test
    public void testRelRestriction() throws JAXBException, UnsupportedEncodingException {
        ForCollection collection = new ForCollection();
        collection.setForCollectionString(ForCollectionContents.CONCEPT.name());

        LetMap letMap = new LetMap();
        LetMap.Map.Entry entry = new LetMap.Map.Entry();
        entry.setKey("acceleration");
        entry.setValue(Snomed.ACCELERATION);
        LetMap.Map.Entry entry2 = new LetMap.Map.Entry();
        entry2.setKey("is a");
        entry2.setValue(Snomed.IS_A);
        LetMap.Map.Entry entry3 = new LetMap.Map.Entry();
        entry3.setKey("motion");
        entry3.setValue(Snomed.MOTION);
        LetMap.Map.Entry entry4 = new LetMap.Map.Entry();
        entry4.setKey("false");
        entry4.setValue(false);

        LetMap.Map map = new LetMap.Map();
        map.getEntry().add(entry);
        map.getEntry().add(entry2);
        map.getEntry().add(entry3);
        map.getEntry().add(entry4);
        letMap.setMap(map);

        // Set the where clause
        Where where = new Where();
        WhereClause relRestriction = new WhereClause();
        relRestriction.setSemanticString(ClauseSemantic.REL_RESTRICTION.name());
        relRestriction.getLetKeys().add("acceleration");
        relRestriction.getLetKeys().add("is a");
        relRestriction.getLetKeys().add("motion");
        relRestriction.getLetKeys().add("false");
        where.setRootClause(relRestriction);

        //String viewpointXml = URLEncoder.encode(getXmlString(viewpoint), "UTF-8");
        //String forObjectString = URLEncoder.encode(getXmlString(forObject), "UTF-8");
        String letMapString = URLEncoder.encode(getXmlString(letMap), "UTF-8");
        String whereString = URLEncoder.encode(getXmlString(where), "UTF-8");
        //String returnTypeString = URLEncoder.encode(getXmlString(returnType), "UTF-8");

        StringBuilder bi = new StringBuilder();
        bi.append("VIEWPOINT=").append("&FOR=").append("&LET=").append(letMapString).append("&WHERE=").append(whereString).append("&RETURN=").append("NIDS");

        System.out.println("Query URL: ");
        System.out.println(bi.toString());
    }

    private static String getXmlString(Object obj) throws JAXBException {
        JAXBContext ctx = JaxbForQuery.get();
        if (obj instanceof SimpleViewCoordinate) {
            org.ihtsdo.otf.jaxb.chronicle.api.ObjectFactory factory = new org.ihtsdo.otf.jaxb.chronicle.api.ObjectFactory();
            obj = factory.createSimpleViewCoordinate((SimpleViewCoordinate) obj);
        } else if (obj instanceof ForCollection) {
            org.ihtsdo.otf.jaxb.query.ObjectFactory factory = new org.ihtsdo.otf.jaxb.query.ObjectFactory();
            obj = factory.createForCollection((ForCollection) obj);
        } else if (obj instanceof org.ihtsdo.otf.jaxb.query.LetMap) {
            org.ihtsdo.otf.jaxb.query.ObjectFactory factory = new org.ihtsdo.otf.jaxb.query.ObjectFactory();
            obj = factory.createLetMap((org.ihtsdo.otf.jaxb.query.LetMap) obj);
        } else if (obj instanceof Where) {
            org.ihtsdo.otf.jaxb.query.ObjectFactory factory = new org.ihtsdo.otf.jaxb.query.ObjectFactory();
            obj = factory.createWhere((Where) obj);
        } else if (obj instanceof ReturnTypes) {
            org.ihtsdo.otf.jaxb.query.ObjectFactory factory = new org.ihtsdo.otf.jaxb.query.ObjectFactory();
            obj = factory.createReturnTypes((ReturnTypes) obj);
        }
        StringWriter writer = new StringWriter();
        ctx.createMarshaller().marshal(obj, writer);
        return writer.toString();
    }

}
