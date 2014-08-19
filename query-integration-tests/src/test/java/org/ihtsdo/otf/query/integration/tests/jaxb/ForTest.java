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
package org.ihtsdo.otf.query.integration.tests.jaxb;

import com.informatics.bdb.junit.ext.BdbTestRunner;
import com.informatics.bdb.junit.ext.BdbTestRunnerConfig;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import org.ihtsdo.otf.query.implementation.ForCollection;
import org.ihtsdo.otf.query.implementation.JaxbForQuery;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.tcc.api.store.TerminologyStoreDI;
import org.ihtsdo.otf.tcc.api.store.Ts;
import org.ihtsdo.otf.tcc.datastore.Bdb;
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
public class ForTest {

    static final TerminologyStoreDI ts = Ts.get();

    public ForTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void conceptTest() {
        try {
            ForCollection forCollection = new ForCollection();
            JAXBContext ctx = JaxbForQuery.get();
            StringWriter writer = new StringWriter();

            ctx.createMarshaller().marshal(forCollection, writer);

            String forXml = writer.toString();
            System.out.println("For list: " + forXml);

            ForCollection unmarshalledForCollection = (ForCollection) ctx.createUnmarshaller()
                    .unmarshal(new StringReader(forXml));
            org.junit.Assert.assertEquals(forCollection.getCollection(), unmarshalledForCollection.getCollection());
        } catch (JAXBException | IOException ex) {
            Logger.getLogger(ForTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Test
    public void getAllComponentsTest() throws IOException {
        ForCollection forCollection = new ForCollection();
        forCollection.setForCollection(ForCollection.ForCollectionContents.COMPONENT);
        NativeIdSetBI forSet = forCollection.getCollection();
        System.out.println(forSet.size());
        assertTrue(forSet.contiguous());
        assertEquals(Bdb.getUuidsToNidMap().getCurrentMaxNid() - Integer.MIN_VALUE, forSet.size());
    }

    @Test
    public void getAllConceptstest() throws IOException {
        ForCollection forCollection = new ForCollection();
        forCollection.setForCollection(ForCollection.ForCollectionContents.CONCEPT);
        NativeIdSetBI forSet = forCollection.getCollection();
        assertEquals(ts.getConceptCount(), forSet.size());
    }

    /**
     * TODO: enable ability write custom FOR sets
     *
     * @throws IOException
     */
    @Ignore
    @Test
    public void getCustomForSetTest() throws IOException {
//        ForCollection forCollection = new ForCollection(Ts.get().isKindOfSet(Snomed.MOTION.getNid(), StandardViewCoordinates.getSnomedInferredLatest()));
//        NativeIdSetBI forSet = forCollection.getCollection();
//        Assert.assertEquals(7, forSet.size());

    }
}
