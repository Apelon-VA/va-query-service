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
package org.ihtsdo.otf.query.rest.client;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import org.ihtsdo.otf.jaxb.chronicle.api.ContradictionManagerPolicy;
import org.ihtsdo.otf.jaxb.chronicle.api.LanguageSort;
import org.ihtsdo.otf.jaxb.chronicle.api.Precedence;
import org.ihtsdo.otf.jaxb.chronicle.api.RelAssertionType;
import org.ihtsdo.otf.jaxb.chronicle.api.SimpleConceptSpecification;
import org.ihtsdo.otf.jaxb.chronicle.api.SimplePath;
import org.ihtsdo.otf.jaxb.chronicle.api.SimplePosition;
import org.ihtsdo.otf.jaxb.chronicle.api.SimpleViewCoordinate;
import org.ihtsdo.otf.jaxb.object.display.ConceptChronicleDdo;
import org.ihtsdo.otf.jaxb.object.display.DescriptionChronicleDdo;
import org.ihtsdo.otf.jaxb.object.display.ResultList;
import org.ihtsdo.otf.jaxb.object.display.SimpleDescriptionVersionDdo;
import org.ihtsdo.otf.jaxb.object.display.SimpleVersionDdo;
import org.ihtsdo.otf.jaxb.query.ForCollection;
import org.ihtsdo.otf.jaxb.query.LetMap;
import org.ihtsdo.otf.jaxb.query.ReturnTypes;
import org.ihtsdo.otf.jaxb.query.Where;

/**
 * Provides a JAXB context object for use by the rest client for converting
 * objects into XML. 
 * @author kec
 */
public class JaxbForClient {
        public static JAXBContext singleton;

    public static JAXBContext get() {
        if (singleton == null) {
            try {
                singleton = JAXBContext.newInstance(ForCollection.class,
                        LetMap.class, SimpleConceptSpecification.class, 
                        ReturnTypes.class, SimpleViewCoordinate.class,
                        SimplePosition.class, 
                        SimplePath.class,
                        ContradictionManagerPolicy.class,
                        LanguageSort.class,
                        Precedence.class, 
                        RelAssertionType.class, 
                        Where.class, 
                        ResultList.class, 
                        ConceptChronicleDdo.class,
                        DescriptionChronicleDdo.class,
                        SimpleVersionDdo.class,
                        SimpleDescriptionVersionDdo.class);
            } catch (JAXBException ex) {
                throw new RuntimeException(ex);
            }
            
        }
        return singleton;
    }
}
