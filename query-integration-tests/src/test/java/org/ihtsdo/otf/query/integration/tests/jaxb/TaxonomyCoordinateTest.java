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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import gov.vha.isaac.metadata.coordinates.LanguageCoordinates;
import gov.vha.isaac.metadata.coordinates.StampCoordinates;
import gov.vha.isaac.metadata.coordinates.TaxonomyCoordinates;
import gov.vha.isaac.ochre.api.coordinate.TaxonomyCoordinate;
import java.io.StringReader;
import java.io.StringWriter;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import org.ihtsdo.otf.query.implementation.JaxbForQuery;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author kec
 */

public class TaxonomyCoordinateTest {


    public TaxonomyCoordinateTest() {
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

            TaxonomyCoordinate originalViewCoordinate = TaxonomyCoordinates.getInferredTaxonomyCoordinate(
                    StampCoordinates.getDevelopmentLatest(), LanguageCoordinates.getUsEnglishLanguageFullySpecifiedNameCoordinate());
            JAXBContext ctx = JaxbForQuery.get();
            StringWriter writer = new StringWriter();

            ctx.createMarshaller().marshal(originalViewCoordinate, writer);

            String viewCoordinateXml = writer.toString();
            System.out.println("ViewCoordinate: " + viewCoordinateXml);

            TaxonomyCoordinate unmarshalledViewCoordinate = (TaxonomyCoordinate) ctx.createUnmarshaller()
                    .unmarshal(new StringReader(viewCoordinateXml));

            assertEquals(originalViewCoordinate, unmarshalledViewCoordinate);
        } catch (JAXBException ex) {
            fail(ex.toString());
        }

    }
}
