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
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.transform.stream.StreamSource;
import org.ihtsdo.otf.jaxb.object.display.ConceptChronicleDdo;
import org.ihtsdo.otf.jaxb.object.display.ResultList;
import org.ihtsdo.otf.jaxb.query.ClauseSemantic;
import org.ihtsdo.otf.jaxb.query.ForCollection;
import org.ihtsdo.otf.jaxb.query.ForCollectionContents;
import org.ihtsdo.otf.jaxb.query.LetMap;
import org.ihtsdo.otf.jaxb.query.ReturnTypes;
import org.ihtsdo.otf.jaxb.query.Where;
import org.ihtsdo.otf.jaxb.query.WhereClause;
import org.ihtsdo.otf.query.rest.client.JaxbForClient;
import org.ihtsdo.otf.query.rest.client.QueryProcessorForRestXml;
import org.ihtsdo.otf.query.rest.client.Snomed;
import org.ihtsdo.otf.query.rest.client.ViewCoordinateExample;

/**
 *
 * @author kec
 */
public class KindOfQueryExample  {

    /**
     *
     * @param args args[0] is an optional server url.
     */
    public static void main(String[] args) {
        try {
            // Construct an example query. 
            
            ForCollection collection = new ForCollection();
            collection.setForCollectionString(ForCollectionContents.CONCEPT.name());
            
            // Setup the let map...
            // Notice the JAXB representation of a map is pretty awkward. Any suggestions
            // for improving the schema generation to make this work better are welcome. 
            LetMap letMap = new LetMap();
            LetMap.Map.Entry entry = new LetMap.Map.Entry();
            entry.setKey("allergic-asthma");
            entry.setValue(Snomed.ALLERGIC_ASTHMA);
            
            LetMap.Map map = new LetMap.Map();
            map.getEntry().add(entry);
            letMap.setMap(map);
            
            // Set the where clause
            Where where = new Where();
            WhereClause conceptIsKindOf = new WhereClause();
            conceptIsKindOf.setSemanticString(ClauseSemantic.CONCEPT_IS_KIND_OF.name());
            conceptIsKindOf.getLetKeys().add("allergic-asthma");
            where.setRootClause(conceptIsKindOf);
            
            // if host is provided, override default host.
            String results;
            if (args.length > 0) {
                results = QueryProcessorForRestXml.process(
                        ViewCoordinateExample.getSnomedInferredLatest(), 
                        collection, letMap, where, ReturnTypes.CONCEPT_VERSION, args[0]);
            } else {
                results = QueryProcessorForRestXml.process(
                        ViewCoordinateExample.getSnomedInferredLatest(), 
                        collection, letMap, where, ReturnTypes.CONCEPT_VERSION);
            }
            
            // print out the XML results as a string...
            Logger.getLogger(KindOfQueryExample.class.getName()).log(Level.INFO, "Results: \n{0}", results);
            
            //Convert the XML to objects using JAXB...
            
            JAXBElement<ResultList> resultsObject = JaxbForClient.get().createUnmarshaller().unmarshal(
                    new StreamSource(new StringReader(results)), ResultList.class);
            
            ResultList theList = resultsObject.getValue();
            for (Object obj: theList.getTheResults()) {
                if (obj instanceof ConceptChronicleDdo) {
                    ConceptChronicleDdo aConcept = (ConceptChronicleDdo) obj;
                    
                    Logger.getLogger(KindOfQueryExample.class.getName()).log(Level.INFO, "Returned concept: {0}", aConcept.getConceptReference().getText());
                } else {
                    Logger.getLogger(KindOfQueryExample.class.getName()).log(Level.INFO, "Returned display object: {0}", obj);
                }
            }


        } catch (IOException | JAXBException ex) {
            Logger.getLogger(KindOfQueryExample.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
