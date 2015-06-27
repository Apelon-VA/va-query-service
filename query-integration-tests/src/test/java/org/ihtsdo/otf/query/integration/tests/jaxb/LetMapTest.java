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

import gov.vha.isaac.metadata.coordinates.ViewCoordinates;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import org.ihtsdo.otf.query.implementation.JaxbForQuery;
import org.ihtsdo.otf.query.implementation.LetMap;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;
import org.ihtsdo.otf.tcc.model.cc.termstore.PersistentStoreI;

import static org.testng.Assert.*;
import org.testng.annotations.*;

/**
 *
 * @author kec
 */
public class LetMapTest {

    private static final Logger LOGGER = Logger.getLogger(LetMapTest.class.getName());
    private static final String DIR = System.getProperty("user.dir");
    private static PersistentStoreI ps;

    public LetMapTest() {
    }

    @BeforeMethod
    public void setUp() {
    }

    @AfterMethod
    public void tearDown() {
    }

    @Test(groups = "QueryServiceTests")
    public void testForMap() {
        try {
            Map<String, Object> map = new HashMap<>();
            map.put("kind-of", Snomed.ALLERGIC_ASTHMA);
            map.put("old-view", ViewCoordinates.getDevelopmentInferredLatestActiveOnly());

            JAXBContext ctx = JaxbForQuery.get();
            StringWriter writer = new StringWriter();

            LetMap wrappedMap = new LetMap(map);
            ctx.createMarshaller().marshal(wrappedMap, writer);

            String forMapXml = writer.toString();
            System.out.println("Map: " + forMapXml);

            LetMap unmarshalledWrappedMap = (LetMap) ctx.createUnmarshaller()
                    .unmarshal(new StringReader(forMapXml));
            assertEquals(map, unmarshalledWrappedMap.getMap());

        } catch (JAXBException ex) {
            Logger.getLogger(LetMapTest.class.getName()).log(Level.SEVERE, null, ex);
            fail(ex.toString());
        }
    }
}
