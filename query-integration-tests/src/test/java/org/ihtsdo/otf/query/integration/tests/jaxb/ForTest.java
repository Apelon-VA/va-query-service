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

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.embed.swing.JFXPanel;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import org.glassfish.hk2.runlevel.RunLevelController;
import org.ihtsdo.otf.query.implementation.ForCollection;
import org.ihtsdo.otf.query.implementation.JaxbForQuery;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.tcc.datastore.Bdb;
import org.ihtsdo.otf.tcc.datastore.BdbTerminologyStore;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.ihtsdo.otf.tcc.model.cc.termstore.PersistentStoreI;

import static org.testng.Assert.*;
import org.testng.annotations.*;

/**
 *
 * @author kec
 */
public class ForTest {

    private static PersistentStoreI ps;

    private static final Logger LOGGER = Logger.getLogger(ForTest.class.getName());
    private static final String DIR = System.getProperty("user.dir");

    public ForTest() {
    }

    @BeforeMethod
    public void setUp() {
    }

    @AfterMethod
    public void tearDown() {
    }

    @Test(groups = "QueryServiceTests")
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
            assertEquals(forCollection.getCollection(), unmarshalledForCollection.getCollection());
        } catch (JAXBException | IOException ex) {
            Logger.getLogger(ForTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Test(groups = "QueryServiceTests")
    public void getAllComponentsTest() throws IOException {
        ForCollection forCollection = new ForCollection();
        forCollection.setForCollection(ForCollection.ForCollectionContents.COMPONENT);
        NativeIdSetBI forSet = forCollection.getCollection();
        System.out.println(forSet.size());
        assertTrue(forSet.contiguous());
        assertEquals(Bdb.getUuidsToNidMap().getCurrentMaxNid() - Integer.MIN_VALUE, forSet.size());
    }

    @Test(groups = "QueryServiceTests")
    public void getAllConceptstest() throws IOException {
        ForCollection forCollection = new ForCollection();
        forCollection.setForCollection(ForCollection.ForCollectionContents.CONCEPT);
        NativeIdSetBI forSet = forCollection.getCollection();
        assertEquals(ps.getConceptCount(), forSet.size());
    }
}
