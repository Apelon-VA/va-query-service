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
import javafx.embed.swing.JFXPanel;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import org.glassfish.hk2.runlevel.RunLevelController;
import org.ihtsdo.otf.query.implementation.JaxbForQuery;
import static gov.vha.isaac.lookup.constants.Constants.CHRONICLE_COLLECTIONS_ROOT_LOCATION_PROPERTY;
import gov.vha.isaac.metadata.coordinates.ViewCoordinates;
import java.util.Set;
import javafx.concurrent.Task;
import java.time.Duration;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ihtsdo.otf.lookup.contracts.contracts.ActiveTaskSet;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.jvnet.testing.hk2testng.HK2;
import org.reactfx.EventStreams;
import org.reactfx.Subscription;

import static org.testng.Assert.*;
import org.testng.annotations.*;

/**
 *
 * @author kec
 */

public class ViewCoordinateTest {


    public ViewCoordinateTest() {
    }


    @BeforeMethod
    public void setUp() throws Exception {

    }

    @AfterMethod
    public void tearDown() throws Exception {

    }
    
    @Test
    public void testJaxb() {
        try {

            ViewCoordinate originalViewCoordinate = ViewCoordinates.getDevelopmentInferredLatestActiveOnly();
            JAXBContext ctx = JaxbForQuery.get();
            StringWriter writer = new StringWriter();

            ctx.createMarshaller().marshal(originalViewCoordinate, writer);

            String viewCoordinateXml = writer.toString();
            System.out.println("ViewCoordinate: " + viewCoordinateXml);

            ViewCoordinate unmarshalledViewCoordinate = (ViewCoordinate) ctx.createUnmarshaller()
                    .unmarshal(new StringReader(viewCoordinateXml));

            assertEquals(originalViewCoordinate, unmarshalledViewCoordinate);
        } catch (JAXBException | IOException ex) {
            fail(ex.toString());
        }

    }
}
