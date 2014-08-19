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
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import org.glassfish.hk2.runlevel.RunLevelController;
import org.ihtsdo.otf.query.implementation.JaxbForQuery;
import org.ihtsdo.otf.query.implementation.LetMap;
import org.ihtsdo.otf.query.implementation.versioning.StandardViewCoordinates;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;
import org.ihtsdo.otf.tcc.datastore.BdbTerminologyStore;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author kec
 */
public class LetMapTest {

    private static final Logger LOGGER = Logger.getLogger(LetMapTest.class.getName());
    private static final String DIR = System.getProperty("user.dir");

    public LetMapTest() {
    }

    @BeforeClass
    public static void setUpClass() {

        LOGGER.log(Level.INFO, "oneTimeSetUp");
        System.setProperty(BdbTerminologyStore.BDB_LOCATION_PROPERTY, DIR + "target/test-resources/berkeley-db");
        RunLevelController runLevelController = Hk2Looker.get().getService(RunLevelController.class);
        LOGGER.log(Level.INFO, "going to run level 1");
        runLevelController.proceedTo(1);
        LOGGER.log(Level.INFO, "going to run level 2");
        runLevelController.proceedTo(2);
    }

    @AfterClass
    public static void tearDownClass() {
        LOGGER.log(Level.INFO, "oneTimeTearDown");
        RunLevelController runLevelController = Hk2Looker.get().getService(RunLevelController.class);
        LOGGER.log(Level.INFO, "going to run level 1");
        runLevelController.proceedTo(1);
        LOGGER.log(Level.INFO, "going to run level 0");
        runLevelController.proceedTo(0);
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testForMap() {
        try {
            Map<String, Object> map = new HashMap<>();
            map.put("kind-of", Snomed.ALLERGIC_ASTHMA);
            map.put("old-view", StandardViewCoordinates.getSnomedInferredLatestActiveOnly());

            JAXBContext ctx = JaxbForQuery.get();
            StringWriter writer = new StringWriter();

            LetMap wrappedMap = new LetMap(map);
            ctx.createMarshaller().marshal(wrappedMap, writer);

            String forMapXml = writer.toString();
            System.out.println("Map: " + forMapXml);

            LetMap unmarshalledWrappedMap = (LetMap) ctx.createUnmarshaller()
                    .unmarshal(new StringReader(forMapXml));
            assertEquals(map, unmarshalledWrappedMap.getMap());

        } catch (IOException | JAXBException ex) {
            Logger.getLogger(LetMapTest.class.getName()).log(Level.SEVERE, null, ex);
            fail(ex.toString());
        }
    }
}
