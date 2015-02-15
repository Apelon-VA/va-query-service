/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.taxonomy;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.cradle.component.ConceptChronicleDataEager;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;
import org.ihtsdo.otf.tcc.api.metadata.binding.SnomedMetadataRf2;
import org.ihtsdo.otf.tcc.api.metadata.binding.TermAux;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.ihtsdo.otf.tcc.model.cc.relationship.Relationship;
import org.ihtsdo.otf.tcc.model.cc.relationship.RelationshipVersion;

/**
 *
 * @author kec
 */
public class TaxonomyCollector implements Collector<ConceptChronicleDataEager, TaxonomyAccumulator, TaxonomyAccumulator> {

    private static final int snomedIsaNid;
    private static final int auxIsaNid;

    private static final int snomedStatedNid;
    private static final int auxStatedNid;
    private static final int snomedInferredNid;
    private static final int auxInferredNid;
    private static final CradleExtensions cradle;
    static {
        try {
            cradle = Hk2Looker.get().getService(CradleExtensions.class);
            snomedIsaNid = cradle.getNidForUuids(Snomed.IS_A.getUuids());
            auxIsaNid = cradle.getNidForUuids(TermAux.IS_A.getUuids());
            snomedStatedNid = cradle.getNidForUuids(SnomedMetadataRf2.STATED_RELATIONSHIP_RF2.getUuids());
            auxStatedNid = cradle.getNidForUuids(TermAux.REL_STATED_CHAR.getUuids());
            snomedInferredNid = cradle.getNidForUuids(SnomedMetadataRf2.INFERRED_RELATIONSHIP_RF2.getUuids());
            auxInferredNid = cradle.getNidForUuids(TermAux.REL_INFERED_CHAR.getUuids());
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
            try {
                int conceptSequence = cradle.getConceptSequence(concept.getNid());
                stats.conceptCount++;
                stats.descCount += concept.getDescriptions().size();
                stats.relCount += concept.getSourceRels().size();


                PrimitiveTaxonomyRecord parentTaxonomyRecord;
                if (stats.taxonomyRecords.containsKey(conceptSequence)) {
                    parentTaxonomyRecord = stats.taxonomyRecords.get(conceptSequence).get();
                } else {
                    parentTaxonomyRecord = new PrimitiveTaxonomyRecord();
                }

                for (Relationship rel : concept.getSourceRels()) {
                    int destinationSequence
                            = cradle.getConceptSequence(rel.getDestinationNid());
                    assert destinationSequence != conceptSequence;
                    PrimitiveTaxonomyRecord childTaxonomyRecord;
                    if (stats.taxonomyRecords.containsKey(destinationSequence)) {
                        childTaxonomyRecord = stats.taxonomyRecords.get(destinationSequence).get();
                    } else {
                        childTaxonomyRecord = new PrimitiveTaxonomyRecord();
                    }
                    
                    rel.getVersions().stream().forEach((rv) -> {
                        stats.relVersionCount++;
                        EnumSet parentRecordFlags = EnumSet.of(TaxonomyFlags.PARENT);
                        EnumSet childRecordFlags = EnumSet.of(TaxonomyFlags.CHILD);
                        if (isaRelType(rv)) {
                            if (statedRelType(rv)) {
                                stats.statedIsaRel++;
                                parentRecordFlags.add(TaxonomyFlags.STATED);
                                childRecordFlags.add(TaxonomyFlags.STATED);
                            } else if (inferredRelType(rv)) {
                                stats.inferredIsaRel++;
                                parentRecordFlags.add(TaxonomyFlags.INFERRED);
                                childRecordFlags.add(TaxonomyFlags.INFERRED);
                            }
                            parentTaxonomyRecord.getTaxonomyRecordUnpacked().addStampRecord(destinationSequence, rv.getStamp(), parentRecordFlags);
                            childTaxonomyRecord.getTaxonomyRecordUnpacked().addStampRecord(conceptSequence, rv.getStamp(), childRecordFlags);
                        }
                    });
                    stats.taxonomyRecords.put(destinationSequence, childTaxonomyRecord);
                }
                stats.taxonomyRecords.put(conceptSequence, parentTaxonomyRecord);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        };
    }

    private static boolean inferredRelType(RelationshipVersion rv) {
        return rv.getCharacteristicNid() == snomedInferredNid
                || rv.getCharacteristicNid() == auxInferredNid;
    }

    private static boolean statedRelType(RelationshipVersion rv) {
        return rv.getCharacteristicNid() == snomedStatedNid
                || rv.getCharacteristicNid() == auxStatedNid;
    }

    private static boolean isaRelType(RelationshipVersion rv) {
        return rv.getTypeNid() == snomedIsaNid || rv.getTypeNid() == auxIsaNid;
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
