/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle;

import gov.vha.isaac.cradle.taxonomy.PrimitiveTaxonomyRecord;
import gov.vha.isaac.cradle.component.ConceptChronicleDataEager;
import gov.vha.isaac.cradle.taxonomy.TaxonomyFlags;
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
import org.ihtsdo.otf.tcc.model.cc.relationship.Relationship;
import org.ihtsdo.otf.tcc.model.cc.relationship.RelationshipVersion;

/**
 *
 * @author kec
 */
public class IsaacStartupCollector implements Collector<ConceptChronicleDataEager, IsaacStartupAccumulator, IsaacStartupAccumulator> {
    
    private final  int snomedIsaNid;
    private final  int auxIsaNid;

    private final  int snomedStatedNid;
    private final  int auxStatedNid;
    private final  int snomedInferredNid;
    private final  int auxInferredNid;
    private final Cradle isaacDb;


    IsaacStartupCollector(Cradle isaacDb) throws IOException {
            this.isaacDb = isaacDb ;
            snomedIsaNid = isaacDb.getNidForUuids(Snomed.IS_A.getUuids());
            auxIsaNid = isaacDb.getNidForUuids(TermAux.IS_A.getUuids());
            snomedStatedNid = isaacDb.getNidForUuids(SnomedMetadataRf2.STATED_RELATIONSHIP_RF2.getUuids());
            auxStatedNid = isaacDb.getNidForUuids(TermAux.REL_STATED_CHAR.getUuids());
            snomedInferredNid = isaacDb.getNidForUuids(SnomedMetadataRf2.INFERRED_RELATIONSHIP_RF2.getUuids());
            auxInferredNid = isaacDb.getNidForUuids(TermAux.REL_INFERED_CHAR.getUuids());
    }
    @Override
    public Supplier<IsaacStartupAccumulator> supplier() {
        return () -> new IsaacStartupAccumulator(isaacDb);
    }

    @Override
    public BiConsumer<IsaacStartupAccumulator, ConceptChronicleDataEager> accumulator() {
        return (IsaacStartupAccumulator stats, ConceptChronicleDataEager concept) -> {
            try {
                int conceptSequence = isaacDb.getConceptSequence(concept.getNid());
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
                            = isaacDb.getConceptSequence(rel.getDestinationNid());
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

    private boolean inferredRelType(RelationshipVersion rv) {
        return rv.getCharacteristicNid() == snomedInferredNid
                || rv.getCharacteristicNid() == auxInferredNid;
    }

    private boolean statedRelType(RelationshipVersion rv) {
        return rv.getCharacteristicNid() == snomedStatedNid
                || rv.getCharacteristicNid() == auxStatedNid;
    }

    private boolean isaRelType(RelationshipVersion rv) {
        return rv.getTypeNid() == snomedIsaNid || rv.getTypeNid() == auxIsaNid;
    }

    @Override
    public BinaryOperator<IsaacStartupAccumulator> combiner() {
        return (a, b) -> a.combine(b);
    }

    @Override
    public Function<IsaacStartupAccumulator, IsaacStartupAccumulator> finisher() {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public Set<Collector.Characteristics> characteristics() {
        return Collections.unmodifiableSet(EnumSet.of(Collector.Characteristics.UNORDERED, Collector.Characteristics.IDENTITY_FINISH));
    }

}
