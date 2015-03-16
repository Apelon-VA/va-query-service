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
import java.util.stream.LongStream;
import org.apache.mahout.math.function.LongProcedure;
import org.apache.mahout.math.set.OpenLongHashSet;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;

/**
 * This class maps stamps to the {@code TaxonomyFlags} associated with that
 * stampSequence.
 *
 * @author kec
 */
public class TypeStampTaxonomyRecords {

    /**
     * int (the map key) is a stampSequence TaxonomyFlags (the map value) are
     * the flags associated with the stampSequence;
     */
    private final OpenLongHashSet typeStampFlagsSet = new OpenLongHashSet(7);

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

    public TypeStampTaxonomyRecords() {
    }

    public TypeStampTaxonomyRecords(int typeSequence, int stampSequence, TaxonomyFlags flag) {
        this.typeStampFlagsSet.add(convertToLong(typeSequence, stampSequence, flag.bits));
    }

    public TypeStampTaxonomyRecords(int[] sourceArray, int sourcePosition) {
        int length = sourceArray[sourcePosition] >>> 24;
        int recordEnd = sourcePosition + length;
        for (sourcePosition = sourcePosition + 1; sourcePosition < recordEnd; sourcePosition += 2) {
            long record = sourceArray[sourcePosition];
            record = record << 32;
            record += sourceArray[sourcePosition + 1];
            typeStampFlagsSet.add(record);
        }
    }

    public LongStream getTypeStampFlagStream() {
        return LongStream.of(typeStampFlagsSet.keys().elements());
    }

    /**
     * 
     * @param typeSequence Integer.MAX_VALUE is a wildcard and will match all types. 
     * @param flags
     * @return true if found. 
     */
    public boolean containsStampOfTypeWithFlags(int typeSequence, int flags) {
        boolean found = !typeStampFlagsSet.forEachKey((long record) -> {
            if (typeSequence == Integer.MAX_VALUE) { // wildcard
                if (((record >>> 32) & TaxonomyRecordPrimitive.FLAGS_BIT_MASK) == flags) {
                    return false; // finish search. 
                }
            } else if ((record & TaxonomyRecordPrimitive.SEQUENCE_BIT_MASK) == typeSequence) {
                if (((record >>> 32) & TaxonomyRecordPrimitive.FLAGS_BIT_MASK) == flags) {
                    return false; // finish search. 
                }
            }
            return true; // continue search...
        });
        return found;
    }

    public IntStream getStampsOfTypeWithFlags(int typeSequence, int flags) {
        Builder intStreamBuilder = IntStream.builder();
        typeStampFlagsSet.forEachKey((long record) -> {
            int stampAndFlag = (int) (record >>> 32);
            if (typeSequence == Integer.MAX_VALUE) { // wildcard
                 if ((stampAndFlag & TaxonomyRecordPrimitive.FLAGS_BIT_MASK) == flags) {
                    intStreamBuilder.accept(stampAndFlag & TaxonomyRecordPrimitive.SEQUENCE_BIT_MASK); 
                }
            } else if ((record & TaxonomyRecordPrimitive.SEQUENCE_BIT_MASK) == typeSequence) {
                if ((stampAndFlag & TaxonomyRecordPrimitive.FLAGS_BIT_MASK) == flags) {
                    intStreamBuilder.accept(stampAndFlag & TaxonomyRecordPrimitive.SEQUENCE_BIT_MASK); 
                }
            }
            return true;
        });
        return intStreamBuilder.build();
    }

    boolean isActive(int typeSequence, TaxonomyCoordinate tc, StampSequenceComputer computer) {
        int flags = TaxonomyFlags.getFlagsFromTaxonomyCoordinate(tc);
        return isActive(typeSequence, flags, computer);
    }

    public boolean isActive(int typeSequence, int flags, StampSequenceComputer computer) {
        int[] latestStamps = computer.getLatestStamps(getStampsOfTypeWithFlags(typeSequence, flags));
        for (int stamp : latestStamps) {
            if (getIsaacDb().getStatusForStamp(stamp) == Status.ACTIVE) {
                return true;
            }
        }
        return false;
    }

    boolean isVisible(int typeSequence, TaxonomyCoordinate tc, StampSequenceComputer computer) {
        int flags = TaxonomyFlags.getFlagsFromTaxonomyCoordinate(tc);
        return isVisible(typeSequence, flags, computer);
    }

    public boolean isVisible(int typeSequence, int flags, StampSequenceComputer computer) {
        return computer.getLatestStamps(getStampsOfTypeWithFlags(typeSequence, flags)).length > 0;
    }

    /**
     *
     * @return the number of integers this stampSequence record will occupy when
     * packed.
     */
    public int length() {
        // 1 is for the concept sequence with the top 8 bits set to the length
        // of sequence plus the associated stampSequence records. 
        return 1 + (typeStampFlagsSet.size() * 2);
    }

    public void addToIntArray(int conceptSequence, int[] destinationArray, int destinationPosition) {
        int length = length();
        int index = destinationPosition + 1;
        destinationArray[destinationPosition] = conceptSequence + (length << 24);
        AddToArrayProcedure addToArrayIntObjectProcedure = new AddToArrayProcedure(index, destinationArray);
        typeStampFlagsSet.forEachKey(addToArrayIntObjectProcedure);
    }

    private static class AddToArrayProcedure implements LongProcedure {

        int index;
        int destinationArray[];

        public AddToArrayProcedure(int index, int[] destinationArray) {
            this.index = index;
            this.destinationArray = destinationArray;
        }

        /**
         * Adds the combined typeSequence + stampSequence + flags to the index location in the
         * destination array defined in the procedure constructor.
         * @param record
         * @return true to continue.
         */
        @Override
        public boolean apply(long record) {
            int stampAndFlags = (int) (record >>> 32);
            destinationArray[index++] = stampAndFlags;
            destinationArray[index++] = (int) record;
            return true;
        }
    }

    public void addStampRecord(int typeSequence, int stampSequence, int taxonomyFlags) {
        long record = convertToLong(typeSequence, stampSequence, taxonomyFlags);
        typeStampFlagsSet.add(record);
    }

    public void merge(TypeStampTaxonomyRecords newRecords) {

        newRecords.typeStampFlagsSet.forEachKey((long recordAsLong) -> {
            typeStampFlagsSet.add(recordAsLong);
            return true;
        });
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        typeStampFlagsSet.forEachKey((long record) -> {
            TypeStampTaxonomyRecord str = new TypeStampTaxonomyRecord(record);
            sb.append(str.toString());
            return true;
        });
        return sb.toString();
    }

    public static long convertToLong(int typeSequence, int stampSequence, int taxonomyFlags) {
        long record = stampSequence;
        if (taxonomyFlags > 512) {
           record += taxonomyFlags;
        } else {
            record += (taxonomyFlags << 24);
        }
        record = record << 32;
        record += typeSequence;
        return record;
    }

    public static class TypeStampTaxonomyRecord implements StampedObject {

        int typeSequence;
        int stampSequence;
        int taxonomyFlags;

        public TypeStampTaxonomyRecord(long record) {
            this.typeSequence = (int) record & TaxonomyRecordPrimitive.SEQUENCE_BIT_MASK;
            record = record >>> 32;
            this.stampSequence = (int) record & TaxonomyRecordPrimitive.SEQUENCE_BIT_MASK;
            this.taxonomyFlags = (int) record & TaxonomyRecordPrimitive.FLAGS_BIT_MASK;
        }

        public TypeStampTaxonomyRecord(int typeSequence, int stampSequence, int taxonomyFlags) {
            this.typeSequence = typeSequence;
            this.stampSequence = stampSequence;
            this.taxonomyFlags = taxonomyFlags;
        }

        public int getTypeSequence() {
            return typeSequence;
        }

        public long getAsLong() {
            return convertToLong(typeSequence, stampSequence, taxonomyFlags);

        }

        @Override
        public int getStamp() {
            return stampSequence;
        }

        public EnumSet<TaxonomyFlags> getTaxonomyFlagsAsEnum() {
            return TaxonomyFlags.getTaxonomyFlags(taxonomyFlags);
        }
        public int getTaxonomyFlags() {
            return taxonomyFlags;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 97 * hash + this.stampSequence;
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
            final TypeStampTaxonomyRecord other = (TypeStampTaxonomyRecord) obj;
            if (this.stampSequence != other.stampSequence) {
                return false;
            }
            if (this.typeSequence != other.typeSequence) {
                return false;
            }
            return this.taxonomyFlags == other.taxonomyFlags;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            try {
                sb.append("«");
                sb.append(getIsaacDb().getConcept(typeSequence).toUserString());
                sb.append("|");
                sb.append(typeSequence);
                sb.append("|");
                sb.append(" ss:");
                sb.append(stampSequence);
                sb.append(" (s:");
                Status status = getIsaacDb().getStatusForStamp(stampSequence);
                sb.append(status);
                sb.append(" t:");
                Instant time = Instant.ofEpochMilli(getIsaacDb().getTimeForStamp(stampSequence));
                sb.append(time.toString());
                sb.append(" a:");
                ConceptChronicleBI author = getIsaacDb().getConcept(getIsaacDb().getAuthorNidForStamp(stampSequence));
                sb.append(author.toUserString());
                sb.append(" m:");
                ConceptChronicleBI module = getIsaacDb().getConcept(getIsaacDb().getModuleNidForStamp(stampSequence));
                sb.append(module.toUserString());
                sb.append(" p:");
                ConceptChronicleBI path = getIsaacDb().getConcept(getIsaacDb().getPathNidForStamp(stampSequence));
                sb.append(path.toUserString());
                sb.append(")->");
                sb.append(getTaxonomyFlagsAsEnum());
                sb.append("»");
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }

            return sb.toString();
        }
    }
}
