/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.tasks;

import gov.vha.isaac.ochre.api.IdentifierService;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.component.concept.ConceptChronology;
import gov.vha.isaac.ochre.api.component.concept.ConceptService;
import gov.vha.isaac.ochre.api.component.concept.ConceptServiceManagerI;
import gov.vha.isaac.ochre.api.component.sememe.SememeChronology;
import gov.vha.isaac.ochre.api.component.sememe.SememeService;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import javafx.concurrent.Task;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ihtsdo.otf.lookup.contracts.contracts.ActiveTaskSet;
import org.ihtsdo.otf.tcc.api.refexDynamic.RefexDynamicChronicleBI;
import org.ihtsdo.otf.tcc.model.cc.refex.RefexMember;
import org.ihtsdo.otf.tcc.model.cc.refex.RefexService;
import org.ihtsdo.otf.tcc.model.index.service.IndexStatusListenerBI;
import org.ihtsdo.otf.tcc.model.index.service.IndexerBI;

/**
 *
 * @author kec
 */
public class GenerateIndexes extends Task<Void> {
    private final static IdentifierService idProvider = LookupService.getService(IdentifierService.class);
//    private final static RefexService refexProvider = LookupService.getService(RefexService.class);
    private final static SememeService sememeProvider = LookupService.getService(SememeService.class);
    private final static ConceptService conceptService = LookupService.getService(ConceptServiceManagerI.class).get();

    private static final Logger log = LogManager.getLogger();

    List<IndexerBI> indexers;
    long componentCount;
    AtomicLong processed = new AtomicLong(0);

    public GenerateIndexes(Class<?> ... indexersToReindex) {
        updateTitle("Index generation");
        updateProgress(-1, Long.MAX_VALUE); // Indeterminate progress
        if (indexersToReindex == null || indexersToReindex.length == 0)
        {
            indexers = LookupService.get().getAllServices(IndexerBI.class);
        }
        else
        {
            indexers = new ArrayList<>();
            for (Class<?> clazz : indexersToReindex)
            {
                if (!IndexerBI.class.isAssignableFrom(clazz))
                {
                    throw new RuntimeException("Invalid Class passed in to the index generator.  Classes must implement IndexerBI ");
                }
                IndexerBI temp = (IndexerBI)LookupService.get().getService(clazz);
                if (temp != null)
                {
                    indexers.add(temp);
                }
            }
        }
        
        List<IndexStatusListenerBI> islList = LookupService.get().getAllServices(IndexStatusListenerBI.class);
        indexers.stream().forEach((i) -> {
            if (islList != null)
            {
                for (IndexStatusListenerBI isl : islList)
                {
                    isl.reindexBegan(i);
                }
            }
            log.info("Clearing index for: " + i.getIndexerName());
            i.clearIndex();
        });

    }

    @Override
    protected Void call() throws Exception {
        LookupService.get().getService(ActiveTaskSet.class).get().add(this);
        try {
            //TODO performance problem... all of these count methods are incredibly slow
            int conceptCount = conceptService.getConceptCount();
            log.info("Concepts to index: " + conceptCount);
//            long refexCount = (int) idProvider.getRefexSequenceStream().count();
//            log.info("Refexes to index: " + refexCount);
            long sememeCount = (int) idProvider.getSememeSequenceStream().count();
            log.info("Sememes to index: " + sememeCount);
            componentCount = conceptCount + sememeCount;
            log.info("Components to index: " + componentCount);
            conceptService.getParallelConceptChronologyStream().forEach((ConceptChronology<?> conceptChronology) -> {
                    indexers.stream().forEach((i) -> {
                        //Currently, our indexers expect descriptions, not concepts... though we might want to re-evaluate this...
                        //I assume that in the future - we will have a description service, rather than embedded descriptions, so leaving as is, for now.
                        conceptChronology.getConceptDescriptionList().forEach((description) -> {
                            i.index(description);
                        });
                    });
                updateProcessedCount();
            });
            
            //TODO Dan - not sure if I actually need either of these, any longer... need to test with SOLOR-ALL 
//            refexProvider.getParallelRefexStream().forEach((RefexMember<?, ?> refex) -> {
//                indexers.stream().forEach((i) -> {
//                    i.index(refex);
//                });
//                updateProcessedCount();
//            });
//            
//            refexProvider.getParallelDynamicRefexStream().forEach((RefexDynamicChronicleBI<?> refex) -> {
//                indexers.stream().forEach((i) -> {
//                    i.index(refex);
//                });
//                updateProcessedCount();
//            });
            
            sememeProvider.getParallelSememeStream().forEach((SememeChronology sememe) -> {
                if (sememe != null)  //TODO Keith -  this IF should not be necessary, but is at the moment, to deal with another bug in sememe provider
                {
                    indexers.stream().forEach((i) -> {
                        i.index(sememe);
                    });
                    updateProcessedCount();
                }
            });
            
            List<IndexStatusListenerBI> islList = LookupService.get().getAllServices(IndexStatusListenerBI.class);

            indexers.stream().forEach((i) -> {
                if (islList != null)
                {
                    islList.stream().forEach((isl) -> {
                        isl.reindexCompleted(i);
                    });
                }
                i.commitWriter();
                i.forceMerge();
            });
            return null;
        } finally {
            LookupService.get().getService(ActiveTaskSet.class).get().remove(this);
        }
    }

    protected void updateProcessedCount() {
        long processedCount = processed.incrementAndGet();
        if (processedCount % 1000 == 0) {
            updateProgress(processedCount, componentCount);
            updateMessage(String.format("Indexed %,d components...", processedCount));
            //We were committing too often every 1000 components, it was bad for performance.
            if (processedCount % 100000 == 0)
            {
                indexers.stream().forEach((i) -> {
                    i.commitWriter();
                });
            }
        }
    }
}
