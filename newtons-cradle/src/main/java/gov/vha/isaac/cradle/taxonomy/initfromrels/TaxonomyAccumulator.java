/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.taxonomy.initfromrels;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordPrimitive;
import gov.vha.isaac.cradle.waitfree.CasSequenceObjectMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;

/**
 *
 * @author kec
 */
public class TaxonomyAccumulator {

    private static final CradleExtensions cradle = Hk2Looker.getService(CradleExtensions.class);

    private static final AtomicInteger idSequence = new AtomicInteger();

    public final int id = idSequence.incrementAndGet();
    ;
    public int descCount = 0;
    public int relCount = 0;
    public int statedIsaRel = 0;
    public int conceptCount = 0;
    public int inferredIsaRel = 0;
    public int otherStatedRel = 0;
    public int otherInferredRel = 0;
    public int relVersionCount = 0;
    public CasSequenceObjectMap<TaxonomyRecordPrimitive> taxonomyRecords;

    public TaxonomyAccumulator() {
        this.taxonomyRecords = cradle.getOriginDestinationTaxonomyMap();
    }

    public TaxonomyAccumulator combine(TaxonomyAccumulator another) {
        this.descCount += another.descCount;
        this.relCount += another.relCount;
        this.statedIsaRel += another.statedIsaRel;
        this.conceptCount += another.conceptCount;
        this.inferredIsaRel += another.inferredIsaRel;
        this.otherStatedRel += another.otherStatedRel;
        this.otherInferredRel += another.otherInferredRel;
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
                ", otherStatedRel=" + otherStatedRel + 
                ", otherInferredRel=" + otherInferredRel + 
                ", relVersionCount=" + relVersionCount + '}';
    }

}
