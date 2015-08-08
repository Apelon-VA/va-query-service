/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.tasks;

import gov.vha.isaac.ochre.api.Get;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.component.concept.ConceptChronology;
import gov.vha.isaac.ochre.api.component.sememe.SememeChronology;
import gov.vha.isaac.ochre.api.task.TimedTask;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ihtsdo.otf.lookup.contracts.contracts.ActiveTaskSet;
import org.ihtsdo.otf.tcc.api.refexDynamic.RefexDynamicChronicleBI;
import org.ihtsdo.otf.tcc.model.cc.refex.RefexService;
import org.ihtsdo.otf.tcc.model.index.service.IndexStatusListenerBI;
import org.ihtsdo.otf.tcc.model.index.service.IndexerBI;

/**
 *
 * @author kec
 */
public class GenerateIndexes extends TimedTask<Void> {
    private final static RefexService refexProvider = LookupService.getService(RefexService.class);

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
            int conceptCount = Get.conceptService().getConceptCount();
            log.info("Concepts to index: " + conceptCount);
            long refexCount = (int) Get.identifierService().getRefexSequenceStream().count();
            log.info("Refexes to index: " + refexCount);
            long sememeCount = (int) Get.identifierService().getSememeSequenceStream().count();
            log.info("Sememes to index: " + sememeCount);
            componentCount = conceptCount + refexCount + sememeCount;
            log.info("Total components to index: " + componentCount);
            Get.conceptService().getParallelConceptChronologyStream().forEach((ConceptChronology<?> conceptChronology) -> {
                    indexers.stream().forEach((i) -> {
                        //Currently, our indexers expect descriptions, not concepts... though we might want to re-evaluate this...
                        //I assume that in the future - we will have a description service, rather than embedded descriptions, so leaving as is, for now.
                        conceptChronology.getConceptDescriptionList().forEach((description) -> {
                            i.index(description);
                        });
                    });
                updateProcessedCount();
            });
            
            //TODO Keith - I don't think we have any old-style refexes any longer, do we?  SCTIDs seem to be coming through the sememe
            //provider below.
//            refexProvider.getParallelRefexStream().forEach((RefexMember<?, ?> refex) -> {
//                indexers.stream().forEach((i) -> {
//                    i.index(refex);
//                });
//                updateProcessedCount();
//            });
            
            //TODO Keith - but I don't understand this bit - this seems to be the only place I can find the dynamic sememes, they aren't coming
            //through the sememe iteration below.
            refexProvider.getParallelDynamicRefexStream().forEach((RefexDynamicChronicleBI<?> refex) -> {
                indexers.stream().forEach((i) -> {
                    i.index(refex);
                });
                updateProcessedCount();
            });
            
            //TODO Keith - why isn't this returning dynamic sememes?
            Get.sememeService().getParallelSememeStream().forEach((SememeChronology sememe) -> {
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
