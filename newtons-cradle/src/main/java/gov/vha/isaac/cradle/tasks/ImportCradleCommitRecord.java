/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.tasks;

import gov.vha.isaac.cradle.log.CradleCommitRecord;
import gov.vha.isaac.ochre.api.Get;
import gov.vha.isaac.ochre.api.commit.CommitService;
import java.util.concurrent.Callable;

/**
 *
 * @author kec
 */
public class ImportCradleCommitRecord implements Callable<Void> {


    CradleCommitRecord ccr;

    public ImportCradleCommitRecord(CradleCommitRecord ccr) {
        this.ccr = ccr;
    }

    @Override
    public Void call() throws Exception {
        CommitService commitManager = Get.commitService();
        String comment = ccr.getCommitComment();
        ccr.getStampsInCommit().stream().forEach((stamp) -> {
            commitManager.setComment(stamp, comment);
        });
        
        ccr.getStampAliases().forEachPair((int stamp, int alias) -> {
            commitManager.addAlias(stamp, alias, comment);
            return true;
        });
        return null;
    }

}