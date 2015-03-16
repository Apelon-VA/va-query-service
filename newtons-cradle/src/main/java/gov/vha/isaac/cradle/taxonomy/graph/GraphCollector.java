/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.taxonomy.graph;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.cradle.taxonomy.TaxonomyFlags;
import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordPrimitive;
import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordUnpacked;
import gov.vha.isaac.cradle.waitfree.CasSequenceObjectMap;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.SequenceProvider;
import gov.vha.isaac.ochre.api.coordinate.TaxonomyCoordinate;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.ObjIntConsumer;
import java.util.stream.IntStream;

import gov.vha.isaac.ochre.api.tree.hashtree.HashTreeBuilder;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import java.io.IOException;
import java.util.UUID;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;

/**
 * Stream-based, parallelizable,  collector to create a graph, which represents a 
 * particular point in time, and a particular semantic state (stated or inferred) 
 * of a taxonomy.
 * @author kec
 */
public class GraphCollector implements 
        ObjIntConsumer<HashTreeBuilder>, BiConsumer<HashTreeBuilder,HashTreeBuilder>{
    private static CradleExtensions isaacDb;

    private final static int ISA_CONCEPT_SEQUENCE = IsaacMetadataAuxiliaryBinding.IS_A.getSequence();

    /**
     * @return the isaacDb
     */
    public static CradleExtensions getIsaacDb() {
        if (isaacDb == null) {
            isaacDb = Hk2Looker.get().getService(CradleExtensions.class);
        }
        return isaacDb;
    }

    final SequenceProvider sequenceProvider = LookupService.getService(SequenceProvider.class);
    final CasSequenceObjectMap<TaxonomyRecordPrimitive> taxonomyMap;
    final TaxonomyCoordinate taxonomyCoordinate;
    final int taxonomyFlags;
    int originSequenceBeingProcessed = -1;
    ConceptSequenceSet watchList = new ConceptSequenceSet();

    public GraphCollector(CasSequenceObjectMap<TaxonomyRecordPrimitive> taxonomyMap, TaxonomyCoordinate viewCoordinate) {
        this.taxonomyMap = taxonomyMap;
        this.taxonomyCoordinate = viewCoordinate;
        taxonomyFlags = TaxonomyFlags.getFlagsFromTaxonomyCoordinate(viewCoordinate);
        addToWatchList("728828c6-b802-3f4e-a45b-3dcf238e48fd");
        addToWatchList("6be31736-e4b2-392f-840a-482393b43d55");
    }

    public final void addToWatchList(String uuid) throws RuntimeException {
        try {
            int nid = getIsaacDb().getNidForUuids(UUID.fromString(uuid));
            watchList.add(sequenceProvider.getConceptSequence(nid));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void accept(HashTreeBuilder graphBuilder, int originSequence) {
        originSequenceBeingProcessed = originSequence;
        if (watchList.contains(originSequence)) {
            System.out.println("Found watch: " + this.toString());
        }
        Optional<TaxonomyRecordPrimitive> isaacPrimitiveTaxonomyRecord = taxonomyMap.get(originSequence);
        
        if (isaacPrimitiveTaxonomyRecord.isPresent()) {
            TaxonomyRecordUnpacked taxonomyRecordUnpacked = isaacPrimitiveTaxonomyRecord.get().getTaxonomyRecordUnpacked();
            IntStream destinationStream = taxonomyRecordUnpacked.getActiveConceptSequencesForType(ISA_CONCEPT_SEQUENCE, taxonomyCoordinate);
            destinationStream.forEach((int destinationSequence) -> graphBuilder.add(destinationSequence, originSequence));
        }
        originSequenceBeingProcessed = -1;
    }

    @Override
    public void accept(HashTreeBuilder t, HashTreeBuilder u) {
        t.combine(u);
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("GraphCollector{");
        buff.append(TaxonomyFlags.getTaxonomyFlags(taxonomyFlags));
        
        if (originSequenceBeingProcessed != -1) {
            try {
                buff.append("} processing: ");
                buff.append(getIsaacDb().getConcept(originSequenceBeingProcessed));
                buff.append(" (");
                buff.append(originSequenceBeingProcessed);
                buff.append(")");
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        } else {
            buff.append("}");
        }
        return buff.toString();
    }

}
