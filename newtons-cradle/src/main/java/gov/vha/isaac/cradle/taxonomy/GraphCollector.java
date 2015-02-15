/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.taxonomy;

import gov.vha.isaac.ochre.api.graph.SimpleDirectedGraphBuilder;
import gov.vha.isaac.cradle.collections.CasSequenceObjectMap;
import gov.vha.isaac.cradle.version.ViewPoint;
import java.util.EnumSet;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.ObjIntConsumer;
import java.util.stream.IntStream;
import org.apache.mahout.math.set.OpenIntHashSet;
import org.ihtsdo.otf.tcc.api.coordinate.Precedence;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;

/**
 *
 * @author kec
 */
public class GraphCollector implements 
        ObjIntConsumer<SimpleDirectedGraphBuilder>, BiConsumer<SimpleDirectedGraphBuilder,SimpleDirectedGraphBuilder>{

    final CasSequenceObjectMap<PrimitiveTaxonomyRecord> taxonomyMap;
    final ViewPoint viewPoint;
    final EnumSet<TaxonomyFlags> taxonomyFlags;

    public GraphCollector(CasSequenceObjectMap<PrimitiveTaxonomyRecord> taxonomyMap, ViewCoordinate viewCoordinate) {
        this.taxonomyMap = taxonomyMap;
        this.viewPoint = new ViewPoint(viewCoordinate.getViewPosition(),
                new OpenIntHashSet(), Precedence.PATH);
        taxonomyFlags = TaxonomyFlags.getFlagsFromRelationshipAssertionType(viewCoordinate);
    }

    @Override
    public void accept(SimpleDirectedGraphBuilder graphBuilder, int conceptSequence) {
        Optional<PrimitiveTaxonomyRecord> isaacPrimitiveTaxonomyRecord = taxonomyMap.get(conceptSequence);
        if (isaacPrimitiveTaxonomyRecord.isPresent()) {
            TaxonomyRecordUnpacked taxonomyRecordUnpacked = isaacPrimitiveTaxonomyRecord.get().getTaxonomyRecordUnpacked();
            IntStream relationStream = taxonomyRecordUnpacked.getActiveConceptSequences(taxonomyFlags, viewPoint);
            if (taxonomyFlags.contains(TaxonomyFlags.PARENT)) {
                relationStream.forEach((int relationshipSequence) -> graphBuilder.add(relationshipSequence, conceptSequence));
            } else {
                relationStream.forEach((int relationshipSequence) -> graphBuilder.add(conceptSequence, relationshipSequence));
            }
        }
    }

    @Override
    public void accept(SimpleDirectedGraphBuilder t, SimpleDirectedGraphBuilder u) {
        t.combine(u);
    }

}
