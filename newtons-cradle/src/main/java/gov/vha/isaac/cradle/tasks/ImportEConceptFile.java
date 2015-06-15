package gov.vha.isaac.cradle.tasks;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.ochre.api.ConceptModel;
import gov.vha.isaac.ochre.api.ConceptProxy;
import gov.vha.isaac.ochre.api.ConfigurationService;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.util.WorkExecutors;
import javafx.concurrent.Task;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ihtsdo.otf.tcc.dto.TtkConceptChronicle;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import javafx.application.Platform;
import org.ihtsdo.otf.lookup.contracts.contracts.ActiveTaskSet;

/**
 * Created by kec on 7/22/14.
 */
public class ImportEConceptFile extends Task<Integer> {

    private static final Logger log = LogManager.getLogger();
    
    Semaphore importPermits = new Semaphore(100);
    
    Path[] paths;
    CradleExtensions termService;
    ConceptProxy stampPath = null;
    UUID stampPathUuid = null;
    ConceptModel conceptModel;

    private ImportEConceptFile(Path[] paths, CradleExtensions termService) {
        updateProgress(-1, Long.MAX_VALUE); // Indeterminate progress
        updateValue(0); // no concepts loaded
        this.paths = paths;
        this.termService = termService;
        this.conceptModel = LookupService.getService(ConfigurationService.class).getConceptModel();
        updateTitle("Concept File Load: " + conceptModel);
    }

    private ImportEConceptFile(Path[] paths, CradleExtensions termService, ConceptProxy stampPath) {
        this(paths, termService);
        this.stampPath = stampPath;
        if (this.stampPath != null) {
            this.stampPathUuid = this.stampPath.getUuids()[0];
        }
    }
    
    public static ImportEConceptFile create(Path[] paths, CradleExtensions termService) {
        ImportEConceptFile importEConceptFile = new ImportEConceptFile(paths, termService);
        LookupService.getService(ActiveTaskSet.class).get().add(importEConceptFile);
        return importEConceptFile;
    }
    public static ImportEConceptFile create(Path[] paths, CradleExtensions termService, ConceptProxy stampPath) {
        ImportEConceptFile importEConceptFile = new ImportEConceptFile(paths, termService, stampPath);
        LookupService.getService(ActiveTaskSet.class).get().add(importEConceptFile);
        return importEConceptFile;
    }

    /*
     Performance notes...

     Using UuidToIntHashMap with load results in:

     Testing load...
     Loaded 402253 concepts in: PT1M39.475S
     nsPerConcept: 247294.61309176066
     msPerConcept: 0.24729461309176065
     concepts in map: 402253

     Using a MapDb ConcurrentNavigableMap<UUID, Integer> uuidNidMap results in:

     Testing load...
     Loaded 402253 concepts in: PT3M52.362S
     nsPerConcept: 577651.3786099793
     msPerConcept: 0.5776513786099793
     concepts in map: 402253

     So the native UuidToIntHashMap is about 2.3x faster.

     When adding parallel processing:

     1 main thread + 1 parallel importer, slightly slower

     Loaded 402253 concepts in: PT1M48.666S
     nsPerConcept: 270143.41720260633
     msPerConcept: 0.2701434172026063
     concepts in map: 402253


     1 main thread + 2 parallel importers is about 1.3 x faster:

     Loaded 402253 concepts in: PT1M14.329S
     nsPerConcept: 184781.71697911512
     msPerConcept: 0.18478171697911513
     concepts in map: 402253

     1 main thread + 32 parallel importers:

     Loaded 402253 concepts in: PT1M26.271S
     nsPerConcept: 214469.50053821848
     msPerConcept: 0.21446950053821848
     concepts in map: 402253

     */
    @Override
    protected Integer call() throws Exception {
        try {
            Instant start = Instant.now();
            ExecutorCompletionService conversionService = new ExecutorCompletionService(LookupService.getService(WorkExecutors.class).getPotentiallyBlockingExecutor());

            AtomicLong bytesToProcessForLoad = new AtomicLong();
            AtomicLong bytesProcessedForLoad = new AtomicLong();
            for (java.nio.file.Path p : paths) {
                bytesToProcessForLoad.addAndGet(p.toFile().length());
                if (conceptModel == ConceptModel.OCHRE_CONCEPT_MODEL) {
                    // Ochre requires a second pass for conversion. 
                    bytesToProcessForLoad.addAndGet(p.toFile().length());
                }
            }

            AtomicInteger conceptCount = new AtomicInteger();
            AtomicInteger completionCount = new AtomicInteger();
            
            switch (conceptModel) {
                case OCHRE_CONCEPT_MODEL:
                    doImport(conceptCount, bytesProcessedForLoad, bytesToProcessForLoad, completionCount, conversionService,
                            "loaded",
                        (TtkConceptChronicle eConcept) -> new ImportEConceptOchreModel(eConcept, stampPathUuid));
                    doImport(conceptCount, bytesProcessedForLoad, bytesToProcessForLoad, completionCount, conversionService,
                            "converted",
                        (TtkConceptChronicle eConcept) -> new ConvertOtfToOchreModel(eConcept, stampPathUuid));
                    break;
                case OTF_CONCEPT_MODEL:
                    doImport(conceptCount, bytesProcessedForLoad, bytesToProcessForLoad, completionCount, conversionService,
                            "loaded",
                        (TtkConceptChronicle eConcept) -> new ImportEConceptOtfModel(eConcept, stampPathUuid));
                    break;
                    default:
                        throw new UnsupportedOperationException("Can't handle: " + conceptModel);
            }
            Instant finish = Instant.now();
            Duration duration = Duration.between(start, finish);

            updateMessage("Load of " + completionCount + " concepts complete in "
                    + duration.getSeconds() + " seconds.");
            updateProgress(bytesToProcessForLoad.get(), bytesToProcessForLoad.get());
            //termService.reportStats();
            return conceptCount.get();
        } finally {
            LookupService.getService(ActiveTaskSet.class).get().remove(this);
        }
    }

    private void doImport(AtomicInteger conceptCount, 
            AtomicLong bytesProcessedForLoad, 
            AtomicLong bytesToProcessForLoad, 
            AtomicInteger completionCount, 
            ExecutorCompletionService conversionService,
            String actionForMessage,
            Function<TtkConceptChronicle, Callable> taskFunction) throws InterruptedException, IOException, ExecutionException, ClassNotFoundException, UnsupportedOperationException {
        for (Path p : paths) {
            long bytesForPath = p.toFile().length();
            try (DataInputStream dis = new DataInputStream(new BufferedInputStream(Files.newInputStream(p)))) {
                updateMessage("Importing file: " + p.toFile().getName());
                // allow for task cancellation, while still importing complete concepts.
                while (!isCancelled()) {
                    long bytesProcessedForPath = bytesForPath - dis.available();
                    if (conceptCount.get() % 1000 == 0) {
                        updateProgress(bytesProcessedForLoad.get() + bytesProcessedForPath, bytesToProcessForLoad.get());
                        
                        updateMessage(String.format("Importing file: " + p.toFile().getName() + 
                                " " + actionForMessage + " %,d concepts...", completionCount.get()));
                        updateValue(completionCount.get());
                    }
                    TtkConceptChronicle eConcept = new TtkConceptChronicle(dis);
                    importPermits.acquireUninterruptibly();
                    conversionService.submit(taskFunction.apply(eConcept));

                                      
                    conceptCount.incrementAndGet();
                    
                    for (Future future = conversionService.poll(); future != null; future = conversionService.poll()) {
                        future.get();
                        importPermits.release();
                        completionCount.incrementAndGet();
                    }
                }
                
            } catch (EOFException eof) {
                // nothing to do.
            }
            
            bytesProcessedForLoad.addAndGet(bytesForPath);
            updateMessage("Importing of file: " + p.toFile().getName() + " complete, cleaning up converters.");
            while (completionCount.get() < conceptCount.get()) {
                Future future = conversionService.take();
                future.get();
                importPermits.release();
                completionCount.incrementAndGet();
            }
         }
    }

    boolean isFxApplicationThread() {
        return Platform.isFxApplicationThread();
    }
}
