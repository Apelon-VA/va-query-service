package gov.vha.isaac.cradle.taxonomy;

import gov.vha.isaac.cradle.version.StampSequenceComputer;
import gov.vha.isaac.cradle.version.ViewPoint;
import org.apache.mahout.math.function.IntObjectProcedure;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenIntObjectHashMap;
import org.ihtsdo.otf.tcc.api.store.Ts;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

/**
 * origin concept sequence->destination concept sequence; stamp; inferred,
 * stated, parent, child
 * <p>
 * <p>
 * Created by kec on 11/8/14.
 */
public class TaxonomyRecordUnpacked {

    /**
     * key = origin concept sequence value = StampRecordsUnpacked
     */
    private final OpenIntObjectHashMap<StampRecordsUnpacked> conceptSequenceRecordMap = new OpenIntObjectHashMap<>(11);

    public TaxonomyRecordUnpacked() {
    }

    public TaxonomyRecordUnpacked(int[] recordArray) {
        if (recordArray != null) {
            int index = 0;
            while (index < recordArray.length) {
                int conceptSequence = recordArray[index] & TaxonomyRecordPacked.sequenceBitmask;
                StampRecordsUnpacked record = new StampRecordsUnpacked(recordArray, index);
                index += record.length();
                conceptSequenceRecordMap.put(conceptSequence, record);
            }
        }
    }

    public Optional<StampRecordsUnpacked> getConceptSequenceStampRecords(int conceptSequence) {
        return Optional.ofNullable(conceptSequenceRecordMap.get(conceptSequence));
    }

    /**
     * @param flags     set of taxonomy flags that concept
     *                  sequence flags must match.
     * @param viewPoint used to
     *                  determine if a concept is active.
     * @return active concepts identified by their sequence value.
     */
    public IntStream getActiveConceptSequences(EnumSet<TaxonomyFlags> flags, ViewPoint viewPoint) {
        StampSequenceComputer computer = StampSequenceComputer.getComputer(viewPoint);
        IntStream.Builder conceptSequenceIntStream = IntStream.builder();
        conceptSequenceRecordMap.forEachPair((int possibleParentSequence, StampRecordsUnpacked stampRecords) -> {
            IntStream.Builder stampsForConceptIntStream = IntStream.builder();
            stampRecords.forEachPair((int stamp, EnumSet<TaxonomyFlags> flagsForStamp) -> {
                if (flagsForStamp.containsAll(flags)) {
                    stampsForConceptIntStream.add(stamp);
                }
                return true;
            });
            if (computer.isLatestActive(stampsForConceptIntStream.build())) {
                conceptSequenceIntStream.accept(possibleParentSequence);
            }
            return true;
        });
        return conceptSequenceIntStream.build();
    }

    public IntStream getVisibleConceptSequences(EnumSet<TaxonomyFlags> flags, ViewPoint viewPoint) {
        StampSequenceComputer computer = StampSequenceComputer.getComputer(viewPoint);
        IntStream.Builder conceptSequenceIntStream = IntStream.builder();
        conceptSequenceRecordMap.forEachPair((int possibleParentSequence, StampRecordsUnpacked stampRecords) -> {
            stampRecords.forEachPair((int stamp, EnumSet<TaxonomyFlags> flagsForStamp) -> {
                if (flagsForStamp.containsAll(flags)) {
                    if (computer.onRoute(stamp)) {
                        conceptSequenceIntStream.accept(possibleParentSequence);
                    }
                    return false;
                }
                return true;
            });
            return true;
        });
        return conceptSequenceIntStream.build();
    }

    public int conectionCount() {
        return conceptSequenceRecordMap.size();
    }

    public void addConceptSequenceStampRecords(int conceptSequence, StampRecordsUnpacked newRecord) {
        if (conceptSequenceRecordMap.containsKey(conceptSequence)) {
            StampRecordsUnpacked oldRecord = conceptSequenceRecordMap.get(conceptSequence);
            oldRecord.merge(newRecord);
        } else {
            conceptSequenceRecordMap.put(conceptSequence, newRecord);
        }
    }

    public void merge(TaxonomyRecordUnpacked newRecord) {
        newRecord.conceptSequenceRecordMap.forEachPair((int key, StampRecordsUnpacked value) -> {
            if (conceptSequenceRecordMap.containsKey(key)) {
                conceptSequenceRecordMap.get(key).merge(value);
            } else {
                conceptSequenceRecordMap.put(key, value);
            }
            return true;
        });
    }

    public int[] pack() {
        PackConceptSequenceStampRecords packer = new PackConceptSequenceStampRecords();
        conceptSequenceRecordMap.forEachPair(packer);
        return packer.taxonomyRecordArray;
    }

    public int length() {
        int length = 0;
        length = conceptSequenceRecordMap.values().stream().map((record) -> record.length()).reduce(length, Integer::sum);
        return length;
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("May change values, can't put in a tree or set");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TaxonomyRecordUnpacked other = (TaxonomyRecordUnpacked) obj;
        return Objects.equals(this.conceptSequenceRecordMap, other.conceptSequenceRecordMap);
    }

    @Override
    public String toString() {
        try {
            IntArrayList theKeys = conceptSequenceRecordMap.keys();
            theKeys.sort();

            StringBuilder buf = new StringBuilder();
            buf.append(" [");
            int maxIndex = theKeys.size() - 1;
            for (int i = 0; i <= maxIndex; i++) {
                int conceptSequence = theKeys.get(i);
                buf.append(String.valueOf(conceptSequenceRecordMap.get(conceptSequence)));
                buf.append("->");
                buf.append(conceptSequence);
                buf.append(" ");
                buf.append(Ts.get().getConcept(conceptSequence));
                 if (i < maxIndex) {
                    buf.append(",");
                }
            }
            buf.append(']');
            return buf.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void addStampRecord(int destinationSequence, int stamp, EnumSet recordFlags) {
        StampRecordsUnpacked conceptSequenceStampRecordsUnpacked;
        if (conceptSequenceRecordMap.containsKey(destinationSequence)) {
            conceptSequenceStampRecordsUnpacked = conceptSequenceRecordMap.get(destinationSequence);
        } else {
            conceptSequenceStampRecordsUnpacked = new StampRecordsUnpacked();
            conceptSequenceRecordMap.put(destinationSequence, conceptSequenceStampRecordsUnpacked);
        }
        conceptSequenceStampRecordsUnpacked.addStampRecord(stamp, recordFlags);
    }

    private class PackConceptSequenceStampRecords implements IntObjectProcedure<StampRecordsUnpacked> {

        int[] taxonomyRecordArray = new int[length()];
        int destinationPosition = 0;

        @Override
        public boolean apply(int conceptSequence, StampRecordsUnpacked stampRecordsUnpacked) {
            stampRecordsUnpacked.addToIntArray(conceptSequence, taxonomyRecordArray, destinationPosition);
            destinationPosition += stampRecordsUnpacked.length();
            return true;
        }
    }
}
