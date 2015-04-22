/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.tasks;

import gov.vha.isaac.cradle.log.CradleCommitRecord;
import gov.vha.isaac.ochre.api.commit.CommitService;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;

/**
 *
 * @author kec
 */
public class ImportCradleCommitRecord implements Callable<Void> {


    CradleCommitRecord ccr;
    Semaphore permit;

    public ImportCradleCommitRecord(CradleCommitRecord ccr,
            Semaphore permit) {
        this.ccr = ccr;
        this.permit = permit;
    }

    @Override
    public Void call() throws Exception {
        try {
            CommitService commitManager = Hk2Looker.getService(CommitService.class);
            String comment = ccr.getCommitComment();
            ccr.getStampsInCommit().forEachKey((int stamp) -> {
                commitManager.setComment(stamp, comment);
                return true;
            });
            
            ccr.getStampAliases().forEachPair((int stamp, int alias) -> {
                commitManager.addAlias(stamp, alias, comment);
                return true;
            });
        } finally {
            permit.release();
        }
        return null;
    }

}