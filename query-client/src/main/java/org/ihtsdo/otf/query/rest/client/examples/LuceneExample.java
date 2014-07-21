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
package org.ihtsdo.otf.query.rest.client.examples;

import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.transform.stream.StreamSource;
import org.ihtsdo.otf.jaxb.object.display.ConceptChronicleDdo;
import org.ihtsdo.otf.jaxb.object.display.ResultList;
import org.ihtsdo.otf.query.rest.client.JaxbForClient;
import org.ihtsdo.otf.query.rest.client.QueryProcessorForRestXml;

/**
 *
 * @author dylangrald
 */
public class LuceneExample {

    public static void main(String[] args) throws UnsupportedEncodingException, JAXBException {
        
        try {
            String results = QueryProcessorForRestXml.lucene("oligophrenia");


            // print out the XML results as a string...
            Logger.getLogger(LuceneExample.class.getName()).log(Level.INFO, "Results: \n{0}", results);

            //Convert the XML to objects using JAXB...

            JAXBElement<ResultList> resultsObject = JaxbForClient.get().createUnmarshaller().unmarshal(
                    new StreamSource(new StringReader(results)), ResultList.class);

            ResultList theList = resultsObject.getValue();
            for (Object obj : theList.getTheResults()) {
                if (obj instanceof ConceptChronicleDdo) {
                    ConceptChronicleDdo aConcept = (ConceptChronicleDdo) obj;

                    Logger.getLogger(LuceneExample.class.getName()).log(Level.INFO, "Returned concept: {0}", aConcept.getConceptReference().getText());
                } else {
                    Logger.getLogger(LuceneExample.class.getName()).log(Level.INFO, "Returned display object: {0}", obj);
                }
            }


        } catch (IOException | JAXBException ex) {
            Logger.getLogger(LuceneExample.class.getName()).log(Level.SEVERE, null, ex);
        }


    }
}
