/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.tasks;

import gov.vha.isaac.cradle.CradleExtensions;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import javafx.concurrent.Task;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ihtsdo.otf.lookup.contracts.contracts.ActiveTaskSet;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.ihtsdo.otf.tcc.dto.TtkConceptChronicle;

/**
 *
 * @author kec
 */
public class ExportEConceptFile extends Task<Integer>{
    Path paths;
    CradleExtensions termService;

    private static final Logger log = LogManager.getLogger();

    int conceptCount;
    AtomicInteger processed = new AtomicInteger(0);
    
    Consumer<TtkConceptChronicle>[] transformers;
    public ExportEConceptFile(Path paths, CradleExtensions termService, Consumer<TtkConceptChronicle>... transformers) {
        this.paths = paths;
        this.termService = termService;
        this.transformers = transformers;
        updateTitle("Export EConcept");
        updateProgress(-1, Long.MAX_VALUE); // Indeterminate progress
        this.termService = termService;
        try {
            conceptCount = termService.getConceptCount();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }


    @Override
    protected Integer call() throws Exception {
        Hk2Looker.get().getService(ActiveTaskSet.class).get().add(this);
      
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(paths.toFile())))) {
            termService.getConceptStream().forEach((ConceptChronicleBI cc) -> {
                try {
                    TtkConceptChronicle eConcept = new TtkConceptChronicle(cc);
                    if (transformers != null) {
                        for (Consumer<TtkConceptChronicle> transformer: transformers) {
                            transformer.accept(eConcept);
                        }
                    }
                    
                    eConcept.writeExternal(dos);
                    
                    int processedCount = processed.incrementAndGet();
                    if (processedCount % 1000 == 0) {
                        updateProgress(processedCount, conceptCount);
                        updateMessage(String.format("Exported %,d concepts...", processedCount));
                    }
                } catch (IOException ex) {
                   throw new RuntimeException(ex);
                }
            });


            return processed.get();
        } finally {
            Hk2Looker.get().getService(ActiveTaskSet.class).get().remove(this);
        }
    }
    
}
