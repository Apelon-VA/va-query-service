/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.taxonomy.walk;

import gov.vha.isaac.cradle.taxonomy.TaxonomyFlags;
import gov.vha.isaac.ochre.api.IdentifierService;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.TaxonomyService;
import gov.vha.isaac.ochre.api.component.concept.ConceptChronology;
import gov.vha.isaac.ochre.api.component.concept.ConceptService;
import gov.vha.isaac.ochre.api.component.concept.ConceptServiceManagerI;
import gov.vha.isaac.ochre.api.coordinate.TaxonomyCoordinate;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.ObjIntConsumer;
import java.util.stream.IntStream;
import org.apache.mahout.math.set.OpenIntHashSet;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;

/**
 *
 * @author kec
 */
public class TaxonomyWalkCollector implements
        ObjIntConsumer<TaxonomyWalkAccumulator>, BiConsumer<TaxonomyWalkAccumulator, TaxonomyWalkAccumulator> {

    private static final IdentifierService identifierService = LookupService.getService(IdentifierService.class);
    private static final ConceptService conceptService = LookupService.getService(ConceptServiceManagerI.class).get();
    private static final int MAX_PRINT_COUNT = 10;

    final TaxonomyService taxonomyService = LookupService.getService(TaxonomyService.class);
    final TaxonomyCoordinate taxonomyCoordinate;
    final int taxonomyFlags;
    final OpenIntHashSet watchSequences = new OpenIntHashSet();
    int errorCount = 0;
    int printCount = 0;

    public TaxonomyWalkCollector(ViewCoordinate viewCoordinate) {
        this.taxonomyCoordinate = viewCoordinate;
        taxonomyFlags = TaxonomyFlags.getFlagsFromTaxonomyCoordinate(viewCoordinate);
        int watchNid = identifierService.getNidForUuids(UUID.fromString("df79ab93-4436-35b8-be3f-2a8e5849d732"));
        watchSequences.add(identifierService.getConceptSequence(watchNid));
    }

    @Override
    public void accept(TaxonomyWalkAccumulator accumulator, int conceptSequence) {

        if (watchSequences.contains(conceptSequence)) {
            accumulator.watchConcept = conceptService.getConcept(conceptSequence);
        } else {
            accumulator.watchConcept = null;
        }

        if (conceptService.isConceptActive(conceptSequence, taxonomyCoordinate.getStampCoordinate())) {
            IntStream parentSequences = taxonomyService.getTaxonomyParentSequencesVisible(conceptSequence, taxonomyCoordinate);
            int parentCount = (int) parentSequences.count();
            if (parentCount == 0) {
                ConceptChronology c = conceptService.getConcept(conceptSequence);               
                if (printCount < MAX_PRINT_COUNT) {
                    printCount++;
                    System.out.println("No parents for: " + c.toUserString());
                }
            }
            accumulator.parentConnections += parentCount;
        }
    }

    @Override
    public void accept(TaxonomyWalkAccumulator t, TaxonomyWalkAccumulator u) {
        t.combine(u);
    }
}
