/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.taxonomy;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.cradle.version.ViewPoint;
import gov.vha.isaac.cradle.version.VisibleStampFilter;
import java.io.IOException;
import java.time.Instant;
import java.util.EnumSet;
import java.util.stream.Stream;
import org.apache.mahout.math.function.IntObjectProcedure;
import org.apache.mahout.math.map.OpenIntObjectHashMap;
import org.apache.mahout.math.set.OpenIntHashSet;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;

/**
 *
 * @author kec
 */
public class StampRecordsUnpacked {


    /**
     * int (the map key) is a stamp
     * TaxonomyFlags (the map value) are the flags associated with the stamp; 
     */
    private final OpenIntObjectHashMap<EnumSet<TaxonomyFlags>> stampRecords = new OpenIntObjectHashMap<>(11);
    
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


    public OpenIntObjectHashMap<EnumSet<TaxonomyFlags>> getStampRecords() {
        return stampRecords;
    }
    
    public OpenIntHashSet stampsForFlags(EnumSet<TaxonomyFlags> flags) {
        OpenIntHashSet stamps = new OpenIntHashSet(stampRecords.size());
        stampRecords.forEachPair((int stamp, EnumSet<TaxonomyFlags> flagsForStamp) -> {
            if (flagsForStamp.containsAll(flags)) {
                stamps.add(stamp);
            }
            return true;
        });
        return stamps;
    }

    public StampRecordsUnpacked() {
    }

    public StampRecordsUnpacked(int stamp, 
            TaxonomyFlags flag) {
        this.stampRecords.put(stamp, EnumSet.of(flag));
    }
    
    public StampRecordsUnpacked(int[] sourceArray, int sourcePosition) {
        int length = sourceArray[sourcePosition] >>> 24;
        int recordEnd = sourcePosition + length; 
        for (sourcePosition = sourcePosition + 1; sourcePosition < recordEnd; sourcePosition++) {
            int stamp = sourceArray[sourcePosition] & TaxonomyRecordPacked.sequenceBitmask;
            EnumSet<TaxonomyFlags> flags = EnumSet.noneOf(TaxonomyFlags.class);
            for (TaxonomyFlags flag: TaxonomyFlags.values()) {
                if ((sourceArray[sourcePosition] & flag.bits) == flag.bits) {
                    flags.add(flag);
                }
                stampRecords.put(stamp, flags);
            }
        }
    }
  
    public Stream<StampTaxonomyRecord> getStampTaxonomyRecordStream() {
        Stream.Builder<StampTaxonomyRecord> builder = Stream.builder();
        stampRecords.forEachPair((int stamp, EnumSet<TaxonomyFlags> flags) -> {
            builder.add(new StampTaxonomyRecord(stamp, flags));
            return true;
        });
        return builder.build();
    }
    
    public Stream<StampTaxonomyRecord> getVisibleStampTaxonomyRecords(ViewPoint vp) {
        return getStampTaxonomyRecordStream().filter(new VisibleStampFilter(vp));
    }
    
    public boolean forEachPair(IntObjectProcedure<EnumSet<TaxonomyFlags>> procedure) {
        return stampRecords.forEachPair(procedure);
    }

    public int length() {
        return 1 + stampRecords.size();
    }
    
    public void addToIntArray(int conceptSequence, int[] destinationArray, int destinationPosition) {
        int length = length();
        int index = destinationPosition + 1;
        destinationArray[destinationPosition] = conceptSequence + (length << 24);
        AddToArrayIntObjectProcedure addToArrayIntObjectProcedure = new AddToArrayIntObjectProcedure(index, destinationArray);
        stampRecords.forEachPair(addToArrayIntObjectProcedure);
    }
    
    private static class AddToArrayIntObjectProcedure implements IntObjectProcedure<EnumSet<TaxonomyFlags>> {

        int index;
        int destinationArray[];

        public AddToArrayIntObjectProcedure(int index, int[] destinationArray) {
            this.index = index;
            this.destinationArray = destinationArray;
        }
        
        @Override
        public boolean apply(int stamp, EnumSet<TaxonomyFlags> flags) {
            stamp = flags.stream().map((field) -> field.bits).reduce(stamp, Integer::sum);
            destinationArray[index] = stamp;
            index++;
            return true;
        }
    
}
   
    public void addStampRecord(int stamp, EnumSet<TaxonomyFlags> fields) {
        if (stampRecords.containsKey(stamp)) {
            stampRecords.get(stamp).addAll(fields);
        } else {
            stampRecords.put(stamp, fields);
        }

    }

    public void merge(StampRecordsUnpacked newRecord) {
        newRecord.stampRecords.forEachPair((int stamp, EnumSet<TaxonomyFlags> flags) -> {
            if (this.stampRecords.containsKey(stamp)) {
                this.stampRecords.get(stamp).addAll(flags);
            } else {
                this.stampRecords.put(stamp, flags);
            }
            return true;
        });
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        stampRecords.forEachPair((int stamp, EnumSet<TaxonomyFlags> flags) -> {
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
                sb.append(flags);
                sb.append("»");
                return true;
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        });
        return sb.toString();
    }
}
