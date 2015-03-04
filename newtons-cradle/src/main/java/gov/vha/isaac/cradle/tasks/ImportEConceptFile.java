package gov.vha.isaac.cradle.tasks;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.ochre.api.ConceptProxy;
import javafx.concurrent.Task;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ihtsdo.otf.tcc.dto.TtkConceptChronicle;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import javafx.application.Platform;
import org.ihtsdo.otf.lookup.contracts.contracts.ActiveTaskSet;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;

/**
 * Created by kec on 7/22/14.
 */
public class ImportEConceptFile extends Task<Integer> {

    private static final Logger log = LogManager.getLogger();

    Path[] paths;
    CradleExtensions termService;
    ConceptProxy stampPath = null;
    UUID stampPathUuid = null;

    public ImportEConceptFile(Path[] paths, CradleExtensions termService) {
        updateTitle("Concept File Load");
        updateProgress(-1, Long.MAX_VALUE); // Indeterminate progress
        updateValue(0); // no concepts loaded
        this.paths = paths;
        this.termService = termService;
    }

    public ImportEConceptFile(Path[] paths, CradleExtensions termService, ConceptProxy stampPath) {
        this(paths, termService);
        this.stampPath = stampPath;
        if (this.stampPath != null) {
            this.stampPathUuid = this.stampPath.getUuids()[0];
        }
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
        Hk2Looker.get().getService(ActiveTaskSet.class).get().add(this);
        try {
            Instant start = Instant.now();
            Semaphore conversionPermits = new Semaphore(Runtime.getRuntime().availableProcessors());
            ExecutorCompletionService conversionService = new ExecutorCompletionService(ForkJoinPool.commonPool());

            long bytesToProcessForLoad = 0;
            long bytesProcessedForLoad = 0;
            for (java.nio.file.Path p : paths) {
                bytesToProcessForLoad += p.toFile().length();
            }

            int conceptCount = 0;
            int completionCount = 0;
            for (Path p : paths) {
                long bytesForPath = p.toFile().length();
                try (DataInputStream dis = new DataInputStream(new BufferedInputStream(Files.newInputStream(p)))) {
                    updateMessage("Importing file: " + p.toFile().getName());
                    // allow for task cancellation, while still importing complete concepts.
                    while (!isCancelled()) {
                        long bytesProcessedForPath = bytesForPath - dis.available();
                        if (conceptCount % 1000 == 0) {
                            updateProgress(bytesProcessedForLoad + bytesProcessedForPath, bytesToProcessForLoad);

                            updateMessage(String.format("Loaded %,d concepts...", completionCount));
                            updateValue(completionCount);
                        }
                        TtkConceptChronicle eConcept = new TtkConceptChronicle(dis);

                        conversionPermits.acquire();
                        conversionService.submit(new ImportEConcept(eConcept, conversionPermits, stampPathUuid));

                        conceptCount++;

                        for (Future future = conversionService.poll(); future != null; future = conversionService.poll()) {
                            future.get();
                            completionCount++;
                        }
                    }

                } catch (EOFException eof) {
                    // nothing to do.
                }

                bytesProcessedForLoad += bytesForPath;
                updateMessage("Importing of file: " + p.toFile().getName() + " complete, cleaning up converters.");
                while (completionCount < conceptCount) {
                    Future future = conversionService.take();
                    future.get();
                    completionCount++;
                }

            }
            Instant finish = Instant.now();
            Duration duration = Duration.between(start, finish);

            updateMessage("Load of " + completionCount + " concepts complete in "
                    + duration.getSeconds() + " seconds.");
            updateProgress(bytesToProcessForLoad, bytesToProcessForLoad);
            //termService.reportStats();
            return conceptCount;
        } finally {
            Hk2Looker.get().getService(ActiveTaskSet.class).get().remove(this);
        }
    }

    boolean isFxApplicationThread() {
        return Platform.isFxApplicationThread();
    }
}
