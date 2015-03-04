/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.taxonomy.initfromrels;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.cradle.component.ConceptChronicleDataEager;
import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordPrimitive;
import gov.vha.isaac.cradle.taxonomy.TaxonomyFlags;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.SequenceProvider;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.ihtsdo.otf.tcc.model.cc.relationship.Relationship;
import org.ihtsdo.otf.tcc.model.cc.relationship.RelationshipVersion;

/**
 *
 * @author kec
 */
public class TaxonomyCollector implements Collector<ConceptChronicleDataEager, TaxonomyAccumulator, TaxonomyAccumulator> {
    private static final SequenceProvider sequenceProvider = Hk2Looker.getService(SequenceProvider.class);
    private static final CradleExtensions cradle = Hk2Looker.getService(CradleExtensions.class);;

    private static final int isaNid;
    private static final int statedNid;
    private static final int inferredNid;
    static {
        try {
            isaNid = cradle.getNidForUuids(IsaacMetadataAuxiliaryBinding.IS_A.getUuids());
            statedNid = cradle.getNidForUuids(IsaacMetadataAuxiliaryBinding.STATED.getUuids());
            inferredNid = cradle.getNidForUuids(IsaacMetadataAuxiliaryBinding.INFERRED.getUuids());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Supplier<TaxonomyAccumulator> supplier() {
        return TaxonomyAccumulator::new;
    }

    @Override
    public BiConsumer<TaxonomyAccumulator, ConceptChronicleDataEager> accumulator() {
        return (TaxonomyAccumulator stats, ConceptChronicleDataEager concept) -> {
            int conceptSequence = sequenceProvider.getConceptSequence(concept.getNid());
            stats.conceptCount++;
            stats.descCount += concept.getDescriptions().size();
            stats.relCount += concept.getSourceRels().size();
            TaxonomyRecordPrimitive parentTaxonomyRecord;
            if (stats.taxonomyRecords.containsKey(conceptSequence)) {
                parentTaxonomyRecord = stats.taxonomyRecords.get(conceptSequence).get();
            } else {
                parentTaxonomyRecord = new TaxonomyRecordPrimitive();
            }
            for (Relationship rel : concept.getSourceRels()) {
                int destinationSequence
                        = sequenceProvider.getConceptSequence(rel.getDestinationNid());
                assert destinationSequence != conceptSequence;
                TaxonomyRecordPrimitive childTaxonomyRecord;
                if (stats.taxonomyRecords.containsKey(destinationSequence)) {
                    childTaxonomyRecord = stats.taxonomyRecords.get(destinationSequence).get();
                } else {
                    childTaxonomyRecord = new TaxonomyRecordPrimitive();
                }
                
                rel.getVersions().stream().forEach((rv) -> {
                    stats.relVersionCount++;
                    if (isaRelType(rv)) {
                    int parentRecordFlags = TaxonomyFlags.PARENT.bits;
                    int childRecordFlags = TaxonomyFlags.CHILD.bits;
                        if (statedRelType(rv)) {
                            stats.statedIsaRel++;
                            parentRecordFlags =+ TaxonomyFlags.STATED.bits;
                            childRecordFlags =+ TaxonomyFlags.STATED.bits;
                        } else if (inferredRelType(rv)) {
                            stats.inferredIsaRel++;
                            parentRecordFlags =+ TaxonomyFlags.INFERRED.bits;
                            childRecordFlags =+ TaxonomyFlags.INFERRED.bits;
                        }
                        parentTaxonomyRecord.getTaxonomyRecordUnpacked().addStampRecord(destinationSequence, rv.getStamp(), parentRecordFlags);
                        childTaxonomyRecord.getTaxonomyRecordUnpacked().addStampRecord(conceptSequence, rv.getStamp(), childRecordFlags);
                    } else {
                        int parentRecordFlags = TaxonomyFlags.OTHER_CONCEPT.bits;
                        int childRecordFlags = TaxonomyFlags.OTHER_CONCEPT.bits;
                        if (statedRelType(rv)) {
                            stats.otherStatedRel++;
                            parentRecordFlags =+ TaxonomyFlags.STATED.bits;
                            childRecordFlags =+ TaxonomyFlags.STATED.bits;
                        } else if (inferredRelType(rv)) {
                            stats.otherInferredRel++;
                            parentRecordFlags =+ TaxonomyFlags.INFERRED.bits;
                            childRecordFlags =+ TaxonomyFlags.INFERRED.bits;
                                    }
                        parentTaxonomyRecord.getTaxonomyRecordUnpacked().addStampRecord(destinationSequence, rv.getStamp(), parentRecordFlags);
                        childTaxonomyRecord.getTaxonomyRecordUnpacked().addStampRecord(conceptSequence, rv.getStamp(), childRecordFlags);
                        
                    }
                });
                stats.taxonomyRecords.put(destinationSequence, childTaxonomyRecord);
            }
            stats.taxonomyRecords.put(conceptSequence, parentTaxonomyRecord);
        };
    }

    private static boolean inferredRelType(RelationshipVersion rv) {
        return rv.getCharacteristicNid() == inferredNid;
    }

    private static boolean statedRelType(RelationshipVersion rv) {
        return rv.getCharacteristicNid() == statedNid;
    }

    private static boolean isaRelType(RelationshipVersion rv) {
        return rv.getTypeNid() == isaNid;
    }

    @Override
    public BinaryOperator<TaxonomyAccumulator> combiner() {
        return (a, b) -> a.combine(b);
    }

    @Override
    public Function<TaxonomyAccumulator, TaxonomyAccumulator> finisher() {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public Set<Collector.Characteristics> characteristics() {
        return Collections.unmodifiableSet(EnumSet.of(Collector.Characteristics.UNORDERED, Collector.Characteristics.IDENTITY_FINISH));
    }

}
