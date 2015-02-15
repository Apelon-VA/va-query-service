/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.taxonomy;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.cradle.collections.CasSequenceObjectMap;
import gov.vha.isaac.cradle.version.ViewPoint;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.ObjIntConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.mahout.math.set.OpenIntHashSet;
import org.ihtsdo.otf.tcc.api.coordinate.Precedence;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.ihtsdo.otf.tcc.model.cc.concept.ConceptChronicle;

/**
 *
 * @author kec
 */
public class TaxonomyWalkCollector implements 
        ObjIntConsumer<TaxonomyWalkAccumulator>, BiConsumer<TaxonomyWalkAccumulator,TaxonomyWalkAccumulator> {

    final CasSequenceObjectMap<PrimitiveTaxonomyRecord> taxonomyMap;
    final ViewPoint viewPoint;
    final EnumSet<TaxonomyFlags> taxonomyFlags;
    final OpenIntHashSet watchSequences = new OpenIntHashSet();

    public TaxonomyWalkCollector(CasSequenceObjectMap<PrimitiveTaxonomyRecord> taxonomyMap, ViewCoordinate viewCoordinate) {
        try {
            this.taxonomyMap = taxonomyMap;
            this.viewPoint = new ViewPoint(viewCoordinate.getViewPosition(),
                    new OpenIntHashSet(), Precedence.PATH);
            taxonomyFlags = TaxonomyFlags.getFlagsFromRelationshipAssertionType(viewCoordinate);
            int watchNid = getCradle().getNidForUuids(UUID.fromString("df79ab93-4436-35b8-be3f-2a8e5849d732"));
            watchSequences.add(getCradle().getConceptSequence(watchNid));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    @Override
    public void accept(TaxonomyWalkAccumulator accumulator, int conceptSequence) {
        
        if (watchSequences.contains(conceptSequence)) {
            try {
                accumulator.watchConcept = (ConceptChronicle) getCradle().getConcept(conceptSequence);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        } else {
            accumulator.watchConcept = null;
        }

        Optional<PrimitiveTaxonomyRecord> primitiveTaxonomyRecord = taxonomyMap.get(conceptSequence);
        if (primitiveTaxonomyRecord.isPresent()) {
            TaxonomyRecordUnpacked taxonomyRecordUnpacked = primitiveTaxonomyRecord.get().getTaxonomyRecordUnpacked();
            accumulator.conceptsProcessed++;
            int connectionCount = taxonomyRecordUnpacked.conectionCount();
            accumulator.connections += connectionCount;
            accumulator.maxConnections = Math.max(accumulator.maxConnections, connectionCount);
            accumulator.minConnections = Math.min(accumulator.minConnections, connectionCount);

            int[] parentSequences = taxonomyRecordUnpacked.getVisibleConceptSequences(TaxonomyFlags.PARENT_FLAG_SET, viewPoint).toArray();

            int depth = Integer.MIN_VALUE;

            accumulator.maxDepth = Math.max(accumulator.maxDepth, depth);
            accumulator.maxDepthSum += depth;

            accumulator.parentConnections += parentSequences.length;
            accumulator.statedParentConnections += taxonomyRecordUnpacked.getVisibleConceptSequences(TaxonomyFlags.STATED_PARENT_FLAGS_SET, viewPoint).count();
            accumulator.inferredParentConnections += taxonomyRecordUnpacked.getVisibleConceptSequences(TaxonomyFlags.INFERRED_PARENT_FLAGS_SET, viewPoint).count();
            accumulator.childConnections += taxonomyRecordUnpacked.getVisibleConceptSequences(TaxonomyFlags.CHILD_FLAG_SET, viewPoint).count();
            accumulator.statedChildConnections += taxonomyRecordUnpacked.getVisibleConceptSequences(TaxonomyFlags.STATED_CHILD_FLAGS_SET, viewPoint).count();
            accumulator.inferredChildConnections += taxonomyRecordUnpacked.getVisibleConceptSequences(TaxonomyFlags.INFERRED_CHILD_FLAGS_SET, viewPoint).count();
        }
    }

    @Override
    public void accept(TaxonomyWalkAccumulator t, TaxonomyWalkAccumulator u) {
        t.combine(u); 
    }
    
    private static CradleExtensions cradle;
    private static CradleExtensions getCradle() {
        if (cradle == null) {
            cradle = Hk2Looker.getService(CradleExtensions.class);
        }
        return cradle;
    }

}
