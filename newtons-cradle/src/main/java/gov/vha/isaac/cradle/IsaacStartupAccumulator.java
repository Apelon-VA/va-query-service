/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle;

import gov.vha.isaac.cradle.taxonomy.PrimitiveTaxonomyRecord;
import gov.vha.isaac.cradle.collections.CasSequenceObjectMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author kec
 */
public class IsaacStartupAccumulator {
    

    private static final AtomicInteger idSequence = new AtomicInteger();

    public final int id = idSequence.incrementAndGet();

    public int descCount = 0;
    public int relCount = 0;
    public int statedIsaRel = 0;
    public int conceptCount = 0;
    public int inferredIsaRel = 0;
    public int relVersionCount = 0;
    public CasSequenceObjectMap<PrimitiveTaxonomyRecord> taxonomyRecords;

    public IsaacStartupAccumulator(Cradle isaacDb) {
        this.taxonomyRecords = isaacDb.getTaxonomyMap();
    }

    public IsaacStartupAccumulator combine(IsaacStartupAccumulator another) {
        this.descCount += another.descCount;
        this.relCount += another.relCount;
        this.statedIsaRel += another.statedIsaRel;
        this.conceptCount += another.conceptCount;
        this.inferredIsaRel += another.inferredIsaRel;
        this.relVersionCount += another.relVersionCount;
        return this;
    }

    @Override
    public String toString() {
        return "StatisticsAccumulator{" + "id=" + id + 
                ", descCount=" + descCount + 
                ", relCount=" + relCount + 
                ", statedIsaRel=" + statedIsaRel + 
                ", conceptCount=" + conceptCount + 
                ", inferredIsaRel=" + inferredIsaRel + 
                ", relVersionCount=" + relVersionCount + '}';
    }

}
