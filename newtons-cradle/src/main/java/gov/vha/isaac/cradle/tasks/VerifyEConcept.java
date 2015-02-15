/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.tasks;

import gov.vha.isaac.cradle.CradleExtensions;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import org.ihtsdo.otf.tcc.dto.TtkConceptChronicle;
import org.ihtsdo.otf.tcc.model.cc.concept.ConceptChronicle;

/**
 *
 * @author kec
 */
public class VerifyEConcept implements Callable<Boolean> {

    CradleExtensions termService;
    TtkConceptChronicle eConcept;
    Semaphore permit;

    public VerifyEConcept(CradleExtensions termService,
            TtkConceptChronicle eConcept,
            Semaphore permit) {
        this.termService = termService;
        this.eConcept = eConcept;
        this.permit = permit;
    }

    @Override
    public Boolean call() throws Exception {
        try {
            int conceptNid = termService.getNidForUuids(eConcept.getPrimordialUuid());

            ConceptChronicle cc = ConceptChronicle.get(conceptNid);
            TtkConceptChronicle remadeEConcept = new TtkConceptChronicle(cc);
            if (!remadeEConcept.equals(eConcept)) {
                System.err.append("Verify failed: " + remadeEConcept);
                return Boolean.FALSE;
            }
            return Boolean.TRUE;
        } finally {
            permit.release();
        }
    }
}

