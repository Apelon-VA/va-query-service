/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.tasks;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.cradle.component.ConceptChronicleDataEager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javafx.concurrent.Task;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ihtsdo.otf.lookup.contracts.contracts.ActiveTaskSet;
import org.ihtsdo.otf.tcc.api.chronicle.ComponentChronicleBI;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.ihtsdo.otf.tcc.model.cc.component.ConceptComponent;
import org.ihtsdo.otf.tcc.model.index.service.IndexStatusListenerBI;
import org.ihtsdo.otf.tcc.model.index.service.IndexerBI;

/**
 *
 * @author kec
 */
public class GenerateIndexes extends Task<Void> {

    private static final Logger log = LogManager.getLogger();

    CradleExtensions termService;
    List<IndexerBI> indexers;
    int conceptCount;
    AtomicInteger processed = new AtomicInteger(0);

    public GenerateIndexes(CradleExtensions termService, Class<?> ... indexersToReindex) {
        updateTitle("Index generation");
        updateProgress(-1, Long.MAX_VALUE); // Indeterminate progress
        this.termService = termService;
        if (indexersToReindex == null || indexersToReindex.length == 0)
        {
            indexers = Hk2Looker.get().getAllServices(IndexerBI.class);
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
                IndexerBI temp = (IndexerBI)Hk2Looker.get().getService(clazz);
                if (temp != null)
                {
                    indexers.add(temp);
                }
            }
        }
        
        List<IndexStatusListenerBI> islList = Hk2Looker.get().getAllServices(IndexStatusListenerBI.class);
        
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
        try {
            conceptCount = termService.getConceptCount();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        Hk2Looker.get().getService(ActiveTaskSet.class).get().add(this);
    }

    @Override
    protected Void call() throws Exception {
        try {
            termService.getParallelConceptDataEagerStream().forEach((ConceptChronicleDataEager ccde) -> {
                ccde.getConceptComponents().forEach((ConceptComponent<?, ?> cc) -> {
                    indexers.stream().forEach((i) -> {
                        i.index((ComponentChronicleBI<?>) cc);
                    });
                });
                int processedCount = processed.incrementAndGet();
                if (processedCount % 1000 == 0) {
                    updateProgress(processedCount, conceptCount);
                    updateMessage(String.format("Indexed %,d concepts...", processedCount));
                    indexers.stream().forEach((i) -> {
                        i.commitWriter();
                    });
                }
            });
            
            List<IndexStatusListenerBI> islList = Hk2Looker.get().getAllServices(IndexStatusListenerBI.class);

            indexers.stream().forEach((i) -> {
                if (islList != null)
                {
                    for (IndexStatusListenerBI isl : islList)
                    {
                        isl.reindexCompleted(i);
                    }
                }
                i.commitWriter();
                i.forceMerge();
            });
            return null;
        } finally {
            Hk2Looker.get().getService(ActiveTaskSet.class).get().remove(this);
        }
    }

}
