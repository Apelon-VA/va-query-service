/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.taxonomy;

import gov.vha.isaac.cradle.waitfree.WaitFreeComparable;
import gov.vha.isaac.ochre.api.coordinate.TaxonomyCoordinate;
import java.util.stream.IntStream;

/**
 *
 * @author kec
 */
public class TaxonomyRecordPrimitive implements WaitFreeComparable {

    public static final int SEQUENCE_BIT_MASK = 0x00FFFFFF;
    public static final int STAMP_BIT_MASK = 0x00FFFFFF;
    public static final int LENGTH_BIT_MASK = 0xFF000000;
    public static final int FLAGS_BIT_MASK = 0xFF000000;

    long msb = 0;
    long lsb = 0;
    int[] taxonomyData;
    transient TaxonomyRecordUnpacked unpacked = null;

    public TaxonomyRecordPrimitive() {
        taxonomyData = new int[0];
    }

    public TaxonomyRecordPrimitive(int[] taxonomyData, long[] md5Data) {

        if (md5Data != null) {
            this.msb = md5Data[0];
            this.lsb = md5Data[1];
        }
        this.taxonomyData = taxonomyData;
    }

    public int[] getArray() {
        if (unpacked != null) {
            taxonomyData = unpacked.pack();
        }
        return taxonomyData;
    }

    @Override
    public long getMd5Msb() {
        return msb;
    }

    @Override
    public long getMd5Lsb() {
        return lsb;
    }

    public TaxonomyRecordUnpacked getTaxonomyRecordUnpacked() {
        if (unpacked != null) {
            return unpacked;
        }
        unpacked = new TaxonomyRecordUnpacked(taxonomyData);
        return unpacked;
    }

    @Override
    public String toString() {
        return getTaxonomyRecordUnpacked().toString();
    }

    public int nextRecordIndex(int index) {
        return taxonomyData[index] >>> 24;
    }

    public int getConceptSequenceIndex(int conceptSequence) {
        throw new UnsupportedOperationException();
    }

    public int getConceptSequence(int index) {
        return taxonomyData[index] & SEQUENCE_BIT_MASK;
    }

    public void addConceptSequenceStampRecord(int[] conceptSequenceStampRecord) {
        conceptSequenceStampRecord[0] = conceptSequenceStampRecord[0]
                + (conceptSequenceStampRecord.length << 24);
    }

    public boolean inferredFlagSet(int index) {
        return (taxonomyData[index] & TaxonomyFlags.INFERRED.bits) == TaxonomyFlags.INFERRED.bits;
    }

    public boolean statedFlagSet(int index) {
        return (taxonomyData[index] & TaxonomyFlags.STATED.bits) == TaxonomyFlags.STATED.bits;
    }

    public int getStamp(int index) {
        // clear any flag bits
        return taxonomyData[index] & SEQUENCE_BIT_MASK;
    }

    public void setStampAndFlags(int index, int stamp, TaxonomyFlags... flags) {
        taxonomyData[index] = stamp;
        for (TaxonomyFlags flag : flags) {
            taxonomyData[index] = taxonomyData[index] + flag.bits;
        }
    }

    public void setSequence(int index, int sequence) {
        taxonomyData[index] = sequence;
    }

    public void setConceptSequenceStampRecordLength(int index, int length) {
        taxonomyData[index] = taxonomyData[index] & SEQUENCE_BIT_MASK;
        length = length << 24;
        taxonomyData[index] = taxonomyData[index] + length;
    }

    public boolean childFlagSet(int index) {
        return (taxonomyData[index] & TaxonomyFlags.CHILD.bits) == TaxonomyFlags.CHILD.bits;
    }

    public boolean parentFlagSet(int index) {
        return (taxonomyData[index] & TaxonomyFlags.PARENT.bits) == TaxonomyFlags.PARENT.bits;
    }
    
    public IntStream getActiveParents(TaxonomyCoordinate tc) {
        return getTaxonomyRecordUnpacked().getActiveConceptSequences(tc);
    }
    public IntStream getVisibleParents(TaxonomyCoordinate tc) {
       return getTaxonomyRecordUnpacked().getVisibleConceptSequences(tc);
    }

    public IntStream getParents() {
       return getTaxonomyRecordUnpacked().getParentConceptSequences();
    }

    public TaxonomyRecordUnpacked unpack() {
        return new TaxonomyRecordUnpacked(taxonomyData);
    }

    public int[] getTaxonomyData() {
        return taxonomyData;
    }

    public boolean containsSequenceWithFlags(int conceptSequence, int flags) {
        return getTaxonomyRecordUnpacked().containsSequenceWithFlags(conceptSequence, flags);
    }
    
    public boolean containsActiveSequence(int conceptSequence, TaxonomyCoordinate tc) {
        return getTaxonomyRecordUnpacked().containsActiveConceptSequence(conceptSequence, tc);
    }
    
    public boolean containsVisibleSequence(int conceptSequence, TaxonomyCoordinate tc) {
        return getTaxonomyRecordUnpacked().containsVisibleConceptSequence(conceptSequence, tc);
    }
    public boolean containsActiveSequence(int conceptSequence, TaxonomyCoordinate tc, int flags) {
        return getTaxonomyRecordUnpacked().containsActiveConceptSequence(conceptSequence, tc, flags);
    }
    
    public boolean containsVisibleSequence(int conceptSequence, TaxonomyCoordinate tc, int flags) {
        return getTaxonomyRecordUnpacked().containsVisibleConceptSequence(conceptSequence, tc, flags);
    }
}
