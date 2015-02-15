package gov.vha.isaac.cradle.taxonomy;

import java.util.Arrays;
import org.apache.mahout.math.list.IntArrayList;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;

/**
 * A <code>TaxonomyRecord</code> is an <code>int[]</code> of variable length containing
 * Concept sequence stamp records, which are also <code>int[]</code> of variable length.<p/>
 * The first <code>int</code> of each Concept sequence stamp record has the concept sequence in the
 * first 16 bits. The remaining 8 bits are an unsigned byte representing the length of the concept
 * sequence stamp record. The remaining <code>int</code> values of the record are STAMPs for which the
 * taxonomy relationship is true, and taxonomy flag bits, per the <code>TaxonomyFlags</code> enumeration;
 *
 * Created by kec on 10/26/14.
 */
public class TaxonomyRecordPacked {


    public static final int sequenceBitmask = 0x00FFFFFF;
    public static final int lengthBitmask   = 0xFF000000;
    protected int[] taxonomyData;



    public TaxonomyRecordPacked(int[] taxonomyData) {
        this.taxonomyData = taxonomyData;
    }

    public int nextRecordIndex(int index) {
        return taxonomyData[index] >>> 24;
    }

    public int getConceptSequenceIndex(int conceptSequence) {
        throw new UnsupportedOperationException();
    }

    public int getConceptSequence(int index) {
        return taxonomyData[index] & sequenceBitmask;
    }

    public void addConceptSequenceStampRecord(int[] conceptSequenceStampRecord) {
        conceptSequenceStampRecord[0] = conceptSequenceStampRecord[0] +
                (conceptSequenceStampRecord.length << 24);
    }

    public boolean inferredFlagSet(int index) {
        return (taxonomyData[index] & TaxonomyFlags.INFERRED.bits) == TaxonomyFlags.INFERRED.bits;
    }

    public boolean statedFlagSet(int index) {
        return (taxonomyData[index] & TaxonomyFlags.STATED.bits) == TaxonomyFlags.STATED.bits;
    }

    public int getStamp(int index) {
        // clear any flag bits
        return taxonomyData[index] & sequenceBitmask;
    }

    public void setStampAndFlags(int index, int stamp, TaxonomyFlags... flags) {
        taxonomyData[index] = stamp;
        for (TaxonomyFlags flag: flags) {
            taxonomyData[index] = taxonomyData[index] + flag.bits;
        }
    }

    public void setSequence(int index, int sequence) {
        taxonomyData[index] = sequence;
    }

    public void setConceptSequenceStampRecordLength(int index, int length) {
        taxonomyData[index] = taxonomyData[index] & sequenceBitmask;
        length = length << 24;
        taxonomyData[index] = taxonomyData[index] + length;
    }

    public boolean childFlagSet(int index) {
        return (taxonomyData[index] & TaxonomyFlags.CHILD.bits) == TaxonomyFlags.CHILD.bits;
    }

    public boolean parentFlagSet(int index) {
        return (taxonomyData[index] & TaxonomyFlags.PARENT.bits) == TaxonomyFlags.PARENT.bits;
    }
    
    public IntArrayList getActiveChildren(ViewCoordinate viewCoordinate) {
        throw new UnsupportedOperationException();
    }

    public IntArrayList getActiveParents(ViewCoordinate viewCoordinate) {
        throw new UnsupportedOperationException();
    }
    
    public TaxonomyRecordUnpacked unpack() {
        return new TaxonomyRecordUnpacked(taxonomyData);
    }

    @Override
    public String toString() {
        return "TaxonomyRecordPacked{" + "taxonomyData=" + Arrays.toString(taxonomyData) + '}';
    }
    
    public int[] getTaxonomyData() {
        return taxonomyData;
    }
}
