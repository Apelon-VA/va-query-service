/*
 * Copyright 2015 U.S. Department of Veterans Affairs.
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
package gov.vha.isaac.cradle.tasks;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.ochre.api.ConceptProxy;
import gov.vha.isaac.ochre.api.Get;
import gov.vha.isaac.ochre.model.concept.ConceptChronologyImpl;
import org.ihtsdo.otf.tcc.dto.TtkConceptChronicle;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * @author kec
 */
public class VerifyEConceptOchreModel implements Callable<Boolean> {

    CradleExtensions termService;
    TtkConceptChronicle eConcept;

    ConceptProxy newPath = null;
    UUID newPathUuid = null;

    public VerifyEConceptOchreModel(CradleExtensions termService,
                                    TtkConceptChronicle eConcept,
                                    ConceptProxy newPath) {
        this(termService, eConcept);
        this.newPath = newPath;
        if (this.newPath != null) {
            this.newPathUuid = this.newPath.getUuids()[0];
        }
    }

    public VerifyEConceptOchreModel(CradleExtensions termService, TtkConceptChronicle eConcept) {
        this.termService = termService;
        this.eConcept = eConcept;
    }

    @Override
    public Boolean call() throws Exception {
        if (this.newPath != null) {
            eConcept.processComponentRevisions(r -> r.setPathUuid(newPathUuid));
        }
        int conceptNid = termService.getNidForUuids(eConcept.getPrimordialUuid());
        ConceptChronologyImpl ochreConcept = (ConceptChronologyImpl) Get.conceptService().getConcept(conceptNid);
        return ochreConcept.getUuidList().equals(eConcept.getUuidList());

//        TtkConceptChronicle remadeEConceptFromOchre = new TtkConceptChronicle(ochreConcept);
//        if (!remadeEConceptFromOchre.equals(eConcept)) {
//            StringBuilder builder = new StringBuilder();
//            builder.append("\n\nVerify failure: ");
//            builder.append(failureCount.incrementAndGet());
//            builder.append(" Remade: \n");
//            builder.append(remadeEConceptFromOchre.toString());
//            builder.append("\nOriginal: \n");
//            builder.append(eConcept);
//            builder.append("\n");
//            System.err.append(builder.toString());
//            return Boolean.FALSE;
//        }
//        return Boolean.TRUE;
    }


}
