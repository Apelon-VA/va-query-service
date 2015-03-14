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
import gov.vha.isaac.ochre.api.SequenceProvider;
import gov.vha.isaac.ochre.api.coordinate.TaxonomyCoordinate;
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
    private static final SequenceProvider sequenceProvider = Hk2Looker.getService(SequenceProvider.class);
    private static final CradleExtensions cradle = Hk2Looker.getService(CradleExtensions.class);;

    final CasSequenceObjectMap<TaxonomyRecordPrimitive> taxonomyMap;
    final ViewCoordinate viewCoordinate;
    final int taxonomyFlags;
    final OpenIntHashSet watchSequences = new OpenIntHashSet();

    public TaxonomyWalkCollector(CasSequenceObjectMap<TaxonomyRecordPrimitive> taxonomyMap, ViewCoordinate viewCoordinate) {
        try {
            this.taxonomyMap = taxonomyMap;
            this.viewCoordinate = viewCoordinate;
            taxonomyFlags = TaxonomyFlags.getFlagsFromTaxonomyCoordinate(viewCoordinate);
            int watchNid = cradle.getNidForUuids(UUID.fromString("df79ab93-4436-35b8-be3f-2a8e5849d732"));
            watchSequences.add(sequenceProvider.getConceptSequence(watchNid));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
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
            if (taxonomyRecordUnpacked.containsActiveConceptSequence(conceptSequence, viewCoordinate, TaxonomyFlags.CONCEPT_STATUS.bits)) {
            int connectionCount = taxonomyRecordUnpacked.conectionCount();
            accumulator.connections += connectionCount;
            accumulator.maxConnections = Math.max(accumulator.maxConnections, connectionCount);
            accumulator.minConnections = Math.min(accumulator.minConnections, connectionCount);

            int[] parentSequences = taxonomyRecordUnpacked.getVisibleConceptSequences(viewCoordinate).toArray();
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
            accumulator.statedParentConnections += taxonomyRecordUnpacked.getVisibleConceptSequences(viewCoordinate).count();
            accumulator.inferredParentConnections += taxonomyRecordUnpacked.getVisibleConceptSequences(viewCoordinate).count();
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
