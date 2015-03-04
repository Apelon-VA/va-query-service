/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.taxonomy;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.cradle.version.StampSequenceComputer;
import gov.vha.isaac.cradle.version.StampedObject;
import gov.vha.isaac.ochre.api.coordinate.TaxonomyCoordinate;
import java.io.IOException;
import java.time.Instant;
import java.util.EnumSet;
import java.util.stream.IntStream;
import java.util.stream.IntStream.Builder;
import org.apache.mahout.math.function.IntIntProcedure;
import org.apache.mahout.math.map.OpenIntIntHashMap;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;

/**
 * This class maps stamps to the
 * {@code TaxonomyFlags} associated with that stamp.
 *
 * @author kec
 */
public class StampTaxonomyRecords {

    /**
     * int (the map key) is a stamp TaxonomyFlags (the map value) are the flags
     * associated with the stamp;
     */
    private final OpenIntIntHashMap stampFlagsMap = new OpenIntIntHashMap(11);

    private static CradleExtensions isaacDb;

    /**
     * @return the isaacDb
     */
    public static CradleExtensions getIsaacDb() {
        if (isaacDb == null) {
            isaacDb = Hk2Looker.get().getService(CradleExtensions.class);
        }
        return isaacDb;
    }

    public StampTaxonomyRecords() {
    }

    public StampTaxonomyRecords(int stamp,
            TaxonomyFlags flag) {
        this.stampFlagsMap.put(stamp, flag.bits);
    }

    public StampTaxonomyRecords(int[] sourceArray, int sourcePosition) {
        int length = sourceArray[sourcePosition] >>> 24;
        int recordEnd = sourcePosition + length;
        for (sourcePosition = sourcePosition + 1; sourcePosition < recordEnd; sourcePosition++) {
            int stamp = sourceArray[sourcePosition] & TaxonomyRecordPrimitive.SEQUENCE_BIT_MASK;
            int flags = sourceArray[sourcePosition] & TaxonomyRecordPrimitive.FLAGS_BIT_MASK;
            stampFlagsMap.put(stamp, flags);
        }
    }
    
    public IntStream getStampFlagStream() {
        int[] keys = stampFlagsMap.keys().elements();
        int[] stampFlags = new int[keys.length];
        for (int i = 0; i < stampFlags.length; i++) {
            stampFlags[i] = keys[i] + stampFlagsMap.get(keys[i]);
        }
        return IntStream.of(stampFlags);
    }

       
    public boolean containsStampWithFlags(int flags) {
        boolean found = !stampFlagsMap.forEachPair((int stamp, int flagsForStamp) -> {
            return (flagsForStamp & flags) != flags;
        });
        return found;
    }
    
    public IntStream getStampsWithFlags(int flags) {
        Builder intStreamBuilder = IntStream.builder();
        stampFlagsMap.forEachPair((int stamp, int flagsForStamp) -> {
            if ((flagsForStamp & flags) != flags) {
                intStreamBuilder.accept(stamp);
            }
            return true;
        });
        return intStreamBuilder.build();
    }

    boolean isActive(TaxonomyCoordinate tc, StampSequenceComputer computer) {
        int flags = TaxonomyFlags.getFlagsFromTaxonomyCoordinate(tc);
        return isActive(flags, computer);
    }

    public boolean isActive(int flags, StampSequenceComputer computer) {
        int[] latestStamps = computer.getLatestStamps(getStampsWithFlags(flags));
        for (int stamp: latestStamps) {
            if (getIsaacDb().getStatusForStamp(stamp) == Status.ACTIVE) {
                return true;
            }
        }
        return false;
    }
    boolean isVisible(TaxonomyCoordinate tc, StampSequenceComputer computer) {
        int flags = TaxonomyFlags.getFlagsFromTaxonomyCoordinate(tc);
        return isVisible(flags, computer);
    }

    public boolean isVisible(int flags, StampSequenceComputer computer) {
        return computer.getLatestStamps(getStampsWithFlags(flags)).length > 0;
    }
    
    /**
     *
     * @return the number of integers this stamp record will occupy when packed.
     */
    public int length() {
        // 1 is for the concept sequence with the top 8 bits set to the length
        // of sequence plus the associated stamp records. 
        return 1 + stampFlagsMap.size();
    }

    public void addToIntArray(int conceptSequence, int[] destinationArray, int destinationPosition) {
        int length = length();
        int index = destinationPosition + 1;
        destinationArray[destinationPosition] = conceptSequence + (length << 24);
        AddToArrayIntIntProcedure addToArrayIntObjectProcedure = new AddToArrayIntIntProcedure(index, destinationArray);
        stampFlagsMap.forEachPair(addToArrayIntObjectProcedure);
    }


    private static class AddToArrayIntIntProcedure implements IntIntProcedure {

        int index;
        int destinationArray[];

        public AddToArrayIntIntProcedure(int index, int[] destinationArray) {
            this.index = index;
            this.destinationArray = destinationArray;
        }

        /**
         * Adds the combined stamp + flags to the index location in the
         * destination array defined in the procedure constructor.
         *
         * @param stamp the stamp to combine with the flags into a single
         * integer.
         * @param flags the flags to combine with the stamp into a single
         * integer.
         * @return true if the procedure should continue
         */
        @Override
        public boolean apply(int stamp, int flags) {
            // This reduce operation is the sum of the flags + the stamp value. 
            // Since the flags only occupy the top 8 bits of the integer, the 
            // bottom 24 bits represent the stamp. 
              stamp += flags;  

            // Tried below streams. Turned out really slow on profiling. So replaced
            // with above. 
            //stamp = flags.stream().map((field) -> field.bits).reduce(stamp, Integer::sum);
            destinationArray[index] = stamp;
            index++;
            return true;
        }
    }

    public void addStampRecord(int stamp, int flags) {
        if (stampFlagsMap.containsKey(stamp)) {
           stampFlagsMap.put(stamp, stampFlagsMap.get(stamp) + flags);
        } else {
            stampFlagsMap.put(stamp, flags);
        }
    }

    public void merge(StampTaxonomyRecords newRecord) {
        newRecord.stampFlagsMap.forEachPair((int stamp, int flags) -> {
            if (stampFlagsMap.containsKey(stamp)) {
                stampFlagsMap.put(stamp, stampFlagsMap.get(stamp) + flags);
            } else {
                stampFlagsMap.put(stamp, flags);
            }
            return true;
        });
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        stampFlagsMap.forEachPair((int stamp, int flags) -> {
            StampTaxonomyRecord str = new StampTaxonomyRecord(stamp, flags);
            sb.append(str.toString());
            return true;
        });
        return sb.toString();
    }

    public static class StampTaxonomyRecord implements StampedObject {

        int stamp;
        int taxonomyFlags;

        public StampTaxonomyRecord(int stamp, int taxonomyFlags) {
            this.stamp = stamp;
            this.taxonomyFlags = taxonomyFlags;
        }

        @Override
        public int getStamp() {
            return stamp;
        }

        public EnumSet<TaxonomyFlags> getTaxonomyFlags() {
            return TaxonomyFlags.getTaxonomyFlags(taxonomyFlags);
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 97 * hash + this.stamp;
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final StampTaxonomyRecord other = (StampTaxonomyRecord) obj;
            if (this.stamp != other.stamp) {
                return false;
            }
            return this.taxonomyFlags == other.taxonomyFlags;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            try {
                sb.append("«");
                sb.append(stamp);
                sb.append(" (s:");
                Status status = getIsaacDb().getStatusForStamp(stamp);
                sb.append(status);
                sb.append(" t:");
                Instant time = Instant.ofEpochMilli(getIsaacDb().getTimeForStamp(stamp));
                sb.append(time.toString());
                sb.append(" a:");
                ConceptChronicleBI author = getIsaacDb().getConcept(getIsaacDb().getAuthorNidForStamp(stamp));
                sb.append(author.toUserString());
                sb.append(" m:");
                ConceptChronicleBI module = getIsaacDb().getConcept(getIsaacDb().getModuleNidForStamp(stamp));
                sb.append(module.toUserString());
                sb.append(" p:");
                ConceptChronicleBI path = getIsaacDb().getConcept(getIsaacDb().getPathNidForStamp(stamp));
                sb.append(path.toUserString());
                sb.append(")->");
                sb.append(getTaxonomyFlags());
                sb.append("»");
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }

            return sb.toString();
        }
    }
}
