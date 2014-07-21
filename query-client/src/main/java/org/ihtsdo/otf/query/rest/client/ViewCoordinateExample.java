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

import javax.xml.bind.JAXBException;
import org.ihtsdo.otf.jaxb.chronicle.api.ContradictionManagerPolicy;
import org.ihtsdo.otf.jaxb.chronicle.api.LanguageSort;
import org.ihtsdo.otf.jaxb.chronicle.api.Precedence;
import org.ihtsdo.otf.jaxb.chronicle.api.RelAssertionType;
import org.ihtsdo.otf.jaxb.chronicle.api.SimpleConceptSpecification;
import org.ihtsdo.otf.jaxb.chronicle.api.SimplePath;
import org.ihtsdo.otf.jaxb.chronicle.api.SimplePosition;
import org.ihtsdo.otf.jaxb.chronicle.api.SimpleViewCoordinate;
import org.ihtsdo.otf.jaxb.chronicle.api.Status;

/**
 *
 * @author kec
 */
public class ViewCoordinateExample {
    
    public static SimpleViewCoordinate getSnomedInferredLatest() throws JAXBException {
        
        
        SimpleViewCoordinate svc = new SimpleViewCoordinate();
        svc.setName("Snomed Inferred Latest");
        svc.setClassifierSpecification(getSpec("IHTSDO Classifier", 
                "7e87cc5b-e85f-3860-99eb-7a44f2b9e6f9"));
        svc.setLanguageSpecification(getSpec("United States of America English language reference set (foundation metadata concept)", 
                "bca0a686-3516-3daf-8fcf-fe396d13cfad"));
        svc.getLanguagePreferenceOrderList().add(svc.getLanguageSpecification());
        svc.getAllowedStatus().add(Status.ACTIVE);
        svc.setPrecedence(Precedence.PATH);
        SimplePath wbAuxPath = new SimplePath();
        wbAuxPath.setPathConceptSpecification(getSpec("Workbench Auxiliary", 
                "2faa9260-8fb2-11db-b606-0800200c9a66"));
        SimplePosition snomedWbAuxOrigin = new SimplePosition();
        snomedWbAuxOrigin.setPath(wbAuxPath);
        // Long.MAX_VALUE == latest
        snomedWbAuxOrigin.setTimePoint(Long.MAX_VALUE);
        
        SimplePath snomedCorePath = new SimplePath();
        snomedCorePath.setPathConceptSpecification(getSpec("SNOMED Core", 
                "8c230474-9f11-30ce-9cad-185a96fd03a2"));
        snomedCorePath.getOrigins().add(snomedWbAuxOrigin);
        
        SimplePosition latestOnSnomedPath = new SimplePosition();
        latestOnSnomedPath.setPath(snomedCorePath);
        latestOnSnomedPath.setTimePoint(Long.MAX_VALUE);
        svc.setViewPosition(latestOnSnomedPath);
        
        svc.setRelAssertionType(RelAssertionType.INFERRED);
        svc.setContradictionPolicy(ContradictionManagerPolicy.LAST_COMMIT_WINS);
        svc.setLangSort(LanguageSort.RF_2_LANG_REFEX);
        
        
        return svc;

    }

    private static SimpleConceptSpecification getSpec(String description, String uuidStr) {
        SimpleConceptSpecification classifierSpec = new SimpleConceptSpecification();
        classifierSpec.setDescription(description);
        classifierSpec.setUuid(uuidStr);
        return classifierSpec;
    }
}