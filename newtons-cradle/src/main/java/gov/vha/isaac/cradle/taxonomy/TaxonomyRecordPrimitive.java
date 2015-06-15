/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.taxonomy;

import gov.vha.isaac.cradle.waitfree.CasSequenceObjectMap;
import gov.vha.isaac.ochre.model.WaitFreeComparable;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.coordinate.StampCoordinate;
import gov.vha.isaac.ochre.api.coordinate.TaxonomyCoordinate;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import java.util.Optional;
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
        
    public static Optional<TaxonomyRecordPrimitive> getIfActiveViaType(int conceptSequence, 
            int typeSequence,
            CasSequenceObjectMap<TaxonomyRecordPrimitive> taxonomyMap, 
            TaxonomyCoordinate vp, int flags) {
        Optional<TaxonomyRecordPrimitive> optionalRecord = taxonomyMap.get(conceptSequence);
        if (optionalRecord.isPresent()) {
            TaxonomyRecordPrimitive record = optionalRecord.get();
            if (record.containsActiveSequenceViaType(conceptSequence, typeSequence,
                    vp, flags)) {
                return optionalRecord;
            }
        }
        return Optional.empty();
    }

    public static Optional<TaxonomyRecordPrimitive> getIfConceptActive(int conceptSequence, 
             CasSequenceObjectMap<TaxonomyRecordPrimitive> taxonomyMap, 
            TaxonomyCoordinate vp) {
        Optional<TaxonomyRecordPrimitive> optionalRecord = taxonomyMap.get(conceptSequence);
        if (optionalRecord.isPresent()) {
            TaxonomyRecordPrimitive record = optionalRecord.get();
            if (record.containsActiveSequenceViaType(conceptSequence, conceptSequence,
                    vp, TaxonomyFlags.CONCEPT_STATUS.bits)) {
                return optionalRecord;
            }
        }
        return Optional.empty();
    }
    
    public static boolean isConceptActive(int conceptSequence, 
             CasSequenceObjectMap<TaxonomyRecordPrimitive> taxonomyMap, 
            StampCoordinate sc) {
        Optional<TaxonomyRecordPrimitive> optionalRecord = taxonomyMap.get(conceptSequence);
        if (optionalRecord.isPresent()) {
            TaxonomyRecordPrimitive record = optionalRecord.get();
            if (record.isConceptActive(conceptSequence, sc)) {
                return true;
            }
        }
        return false;
    }

    int writeSequence;
    int[] taxonomyData;
    transient TaxonomyRecordUnpacked unpacked = null;

    public TaxonomyRecordPrimitive() {
        taxonomyData = new int[0];
    }

    public TaxonomyRecordPrimitive(int[] taxonomyData, int writeSequence) {

        this.taxonomyData = taxonomyData;
        this.writeSequence = writeSequence;
    }

    public int[] getArray() {
        if (unpacked != null) {
            taxonomyData = unpacked.pack();
        }
        return taxonomyData;
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
            taxonomyData[index] = taxonomyData[index] | flag.bits;
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
    
    public IntStream getActiveParentSequences(TaxonomyCoordinate tc) {
        return getTaxonomyRecordUnpacked().getActiveConceptSequencesForType(IsaacMetadataAuxiliaryBinding.IS_A.getSequence(), tc);
    }
    public IntStream getVisibleParentSequences(TaxonomyCoordinate tc) {
       return getTaxonomyRecordUnpacked().getVisibleConceptSequencesForType(IsaacMetadataAuxiliaryBinding.IS_A.getSequence(), tc);
    }

    public IntStream getParentSequences() {
       return getTaxonomyRecordUnpacked().getParentConceptSequences();
    }

    public IntStream getDestinationSequences() {
       return getTaxonomyRecordUnpacked().getDestinationConceptSequences();
    }

    public IntStream getDestinationSequencesOfType(ConceptSequenceSet typeSequenceSet) {
       return getTaxonomyRecordUnpacked().getDestinationConceptSequencesOfType(typeSequenceSet);
    }

    public IntStream getDestinationSequencesOfTypeVisible(ConceptSequenceSet typeSequenceSet, TaxonomyCoordinate tc) {
       return getTaxonomyRecordUnpacked().getDestinationConceptSequencesOfTypeVisible(typeSequenceSet, tc);
    }

    public IntStream getDestinationSequencesOfTypeActive(ConceptSequenceSet typeSequenceSet, TaxonomyCoordinate tc) {
       return getTaxonomyRecordUnpacked().getDestinationConceptSequencesOfTypeActive(typeSequenceSet, tc);
    }

    public TaxonomyRecordUnpacked unpack() {
        return new TaxonomyRecordUnpacked(taxonomyData);
    }

    public int[] getTaxonomyData() {
        return taxonomyData;
    }

    public boolean containsSequenceViaTypeWithFlags(int conceptSequence, int typeSequence, int flags) {
        return getTaxonomyRecordUnpacked().containsSequenceViaTypeWithFlags(conceptSequence, typeSequence, flags);
    }
    
    public boolean containsActiveSequenceViaType(int conceptSequence, int typeSequence, TaxonomyCoordinate tc) {
        return getTaxonomyRecordUnpacked().containsActiveConceptSequenceViaType(conceptSequence, typeSequence, tc);
    }
    
    public boolean containsActiveSequenceViaType(int conceptSequence, ConceptSequenceSet typeSequenceSet, TaxonomyCoordinate tc) {
        return getTaxonomyRecordUnpacked().containsActiveConceptSequenceViaType(conceptSequence, typeSequenceSet, tc);
    }
    
    public boolean containsVisibleSequenceViaType(int conceptSequence, int typeSequence, TaxonomyCoordinate tc) {
        return getTaxonomyRecordUnpacked().containsVisibleConceptSequenceViaType(conceptSequence, typeSequence, tc);
    }

    public boolean containsVisibleSequenceViaType(int conceptSequence, ConceptSequenceSet typeSequenceSet, TaxonomyCoordinate tc) {
        return getTaxonomyRecordUnpacked().containsVisibleConceptSequenceViaType(conceptSequence, typeSequenceSet, tc);
    }

    public boolean containsActiveSequenceViaType(int conceptSequence, int typeSequence, TaxonomyCoordinate tc, int flags) {
        return getTaxonomyRecordUnpacked().containsActiveConceptSequenceViaType(conceptSequence, typeSequence, tc, flags);
    }
    
    public boolean containsActiveSequenceViaType(int conceptSequence, ConceptSequenceSet typeSequenceSet, TaxonomyCoordinate tc, int flags) {
        return getTaxonomyRecordUnpacked().containsActiveConceptSequenceViaType(conceptSequence, typeSequenceSet, tc, flags);
    }
    
    public boolean isConceptActive(int conceptSequence, StampCoordinate stampCoordinate) {
        return getTaxonomyRecordUnpacked().isConceptActive(conceptSequence, stampCoordinate);
    }
    
    public boolean containsVisibleSequenceViaType(int conceptSequence, int typeSequence, TaxonomyCoordinate tc, int flags) {
        return getTaxonomyRecordUnpacked().containsVisibleConceptSequenceViaType(conceptSequence, typeSequence, tc, flags);
    }

    public boolean containsVisibleSequenceViaType(int conceptSequence, ConceptSequenceSet typeSequenceSet, TaxonomyCoordinate tc, int flags) {
        return getTaxonomyRecordUnpacked().containsVisibleConceptSequenceViaType(conceptSequence, typeSequenceSet, tc, flags);
    }
    public boolean containsSequenceViaType(int conceptSequence, ConceptSequenceSet typeSequenceSet, int flags) {
        return getTaxonomyRecordUnpacked().containsConceptSequenceViaType(conceptSequence, typeSequenceSet, flags);
    }

    @Override
    public int getWriteSequence() {
        return writeSequence;
    }

    @Override
    public void setWriteSequence(int sequence) {
        this.writeSequence = sequence;
    }
}
