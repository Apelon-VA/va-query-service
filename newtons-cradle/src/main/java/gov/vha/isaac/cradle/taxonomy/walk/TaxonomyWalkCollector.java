/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.taxonomy.walk;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordPrimitive;
import gov.vha.isaac.cradle.taxonomy.TaxonomyFlags;
import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordUnpacked;
import gov.vha.isaac.cradle.waitfree.CasSequenceObjectMap;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.IdentifierService;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.ObjIntConsumer;
import org.apache.mahout.math.set.OpenIntHashSet;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.ihtsdo.otf.tcc.model.cc.concept.ConceptChronicle;

/**
 *
 * @author kec
 */
public class TaxonomyWalkCollector implements 
        ObjIntConsumer<TaxonomyWalkAccumulator>, BiConsumer<TaxonomyWalkAccumulator,TaxonomyWalkAccumulator> {
    private static final IdentifierService sequenceProvider = Hk2Looker.getService(IdentifierService.class);
    private static final CradleExtensions cradle = Hk2Looker.getService(CradleExtensions.class);;
    private static final int ISA_CONCEPT_SEQUENCE = IsaacMetadataAuxiliaryBinding.IS_A.getSequence();

    final CasSequenceObjectMap<TaxonomyRecordPrimitive> taxonomyMap;
    final ViewCoordinate viewCoordinate;
    final int taxonomyFlags;
    final OpenIntHashSet watchSequences = new OpenIntHashSet();

    public TaxonomyWalkCollector(CasSequenceObjectMap<TaxonomyRecordPrimitive> taxonomyMap, ViewCoordinate viewCoordinate) {
        this.taxonomyMap = taxonomyMap;
        this.viewCoordinate = viewCoordinate;
        taxonomyFlags = TaxonomyFlags.getFlagsFromTaxonomyCoordinate(viewCoordinate);
        int watchNid = cradle.getNidForUuids(UUID.fromString("df79ab93-4436-35b8-be3f-2a8e5849d732"));
        watchSequences.add(sequenceProvider.getConceptSequence(watchNid));
    }
    
    @Override
    public void accept(TaxonomyWalkAccumulator accumulator, int conceptSequence) {
        
        if (watchSequences.contains(conceptSequence)) {
            try {
                accumulator.watchConcept = (ConceptChronicle) cradle.getConcept(conceptSequence);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        } else {
            accumulator.watchConcept = null;
        }

        Optional<TaxonomyRecordPrimitive> primitiveTaxonomyRecord = taxonomyMap.get(conceptSequence);
        if (primitiveTaxonomyRecord.isPresent()) {
            TaxonomyRecordUnpacked taxonomyRecordUnpacked = primitiveTaxonomyRecord.get().getTaxonomyRecordUnpacked();
            accumulator.conceptsProcessed++;
            if (taxonomyRecordUnpacked.containsActiveConceptSequenceViaType(conceptSequence, Integer.MAX_VALUE,
                    viewCoordinate, TaxonomyFlags.CONCEPT_STATUS.bits)) {
            int connectionCount = taxonomyRecordUnpacked.conectionCount();
            accumulator.connections += connectionCount;
            accumulator.maxConnections = Math.max(accumulator.maxConnections, connectionCount);
            accumulator.minConnections = Math.min(accumulator.minConnections, connectionCount);

            int[] parentSequences = 
                    taxonomyRecordUnpacked.getVisibleConceptSequencesForType(ISA_CONCEPT_SEQUENCE, viewCoordinate).toArray();
            if (parentSequences.length == 0) {
                try {
                    ConceptChronicleBI c = cradle.getConcept(conceptSequence);
                    System.out.println("No parents for: " + c);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }

            int depth = Integer.MIN_VALUE;

            accumulator.maxDepth = Math.max(accumulator.maxDepth, depth);
            accumulator.maxDepthSum += depth;

            accumulator.parentConnections += parentSequences.length;
            accumulator.statedParentConnections += taxonomyRecordUnpacked.getVisibleConceptSequencesForType(ISA_CONCEPT_SEQUENCE, viewCoordinate).count();
            accumulator.inferredParentConnections += taxonomyRecordUnpacked.getVisibleConceptSequencesForType(ISA_CONCEPT_SEQUENCE, viewCoordinate).count();
        }
        } else {
            try {
                System.out.println("No ptm for: " + cradle.getConcept(conceptSequence).toLongString());
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public void accept(TaxonomyWalkAccumulator t, TaxonomyWalkAccumulator u) {
        t.combine(u); 
    }
}
