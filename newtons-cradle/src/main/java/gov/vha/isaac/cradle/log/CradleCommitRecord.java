/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.log;

import gov.vha.isaac.cradle.version.StampUniversal;
import gov.vha.isaac.ochre.api.commit.CommitRecord;
import gov.vha.isaac.ochre.collections.StampSequenceSet;
import java.io.DataInput;
import java.io.IOException;
import java.time.Instant;
import org.apache.mahout.math.map.OpenIntIntHashMap;
import org.apache.mahout.math.set.OpenIntHashSet;
import org.ihtsdo.otf.tcc.model.cc.termstore.Termstore;

/**
 *
 * @author kec
 */
public class CradleCommitRecord extends CommitRecord {

    public CradleCommitRecord(Instant commitTime, OpenIntHashSet stampsInCommit, 
            OpenIntIntHashMap stampAliases, String commitComment) {
        throw new UnsupportedOperationException(commitComment);
                
        //super(commitTime, stampsInCommit, stampAliases, commitComment);
    }

    public CradleCommitRecord(DataInput in, Termstore store) throws IOException {
        this.commitComment = in.readUTF();
        this.commitTime = Instant.ofEpochSecond(in.readLong(), in.readLong());
        int stampAliasesCount = in.readInt();
        this.stampAliases = new OpenIntIntHashMap(stampAliasesCount);
        for (int i = 0; i < stampAliasesCount; i++) {
            StampUniversal stampAlias = new StampUniversal(in);
            StampUniversal primordialStamp = new StampUniversal(in);
            stampAliases.put(store.getStamp(stampAlias), store.getStamp(primordialStamp));
        }
        int stampsInCommitCount = in.readInt();
        this.stampsInCommit = new StampSequenceSet();
        for (int i = 0; i < stampsInCommitCount; i++) {
            StampUniversal stamp = new StampUniversal(in);
            stampsInCommit.add(store.getStamp(stamp));
        }
        throw new UnsupportedOperationException(commitComment);
    }
}
