/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.tasks;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.ochre.api.ConceptProxy;
import javafx.application.Platform;
import javafx.concurrent.Task;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ihtsdo.otf.lookup.contracts.contracts.ActiveTaskSet;
import org.ihtsdo.otf.tcc.dto.TtkConceptChronicle;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;

/**
 *
 * @author kec
 */
public class VerifyLoadEConceptFile
        extends Task<Boolean> {

    private static final Logger log = LogManager.getLogger();
    private static final Set<UUID> watchSet = new HashSet<>();
//            new HashSet<>(Arrays.asList(UUID.fromString("bcefc7ae-7512-3893-ade1-8eae817b4f0d"), 
//            UUID.fromString("afee3454-a2ef-3d89-a51e-c3337317296a")));

    Path[] paths;
    CradleExtensions termService;
    ConceptProxy stampPath = null;


    public VerifyLoadEConceptFile(Path[] paths, CradleExtensions termService) {
        updateTitle("Verify load of concept files");
        updateProgress(-1, Long.MAX_VALUE); // Indeterminate progress
        this.paths = paths;
        this.termService = termService;
        Hk2Looker.get().getService(ActiveTaskSet.class).get().add(this);
    }

    public VerifyLoadEConceptFile(Path[] paths, CradleExtensions termService, ConceptProxy stampPath) {
        this(paths, termService);
        this.stampPath = stampPath;
    }

    @Override
    protected Boolean call() throws Exception {
        boolean filesVerified = true;
        try {
            Instant start = Instant.now();
            Semaphore conversionPermits = new Semaphore(Runtime.getRuntime().availableProcessors());
            ExecutorCompletionService<Boolean> conversionService = new ExecutorCompletionService(ForkJoinPool.commonPool());

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
                    updateMessage("Verifying file: " + p.toFile().getName());
                    // allow for task cancellation, while still importing complete concepts.
                    while (!isCancelled()) {
                        long bytesProcessedForPath = bytesForPath - dis.available();
                        if (conceptCount % 1000 == 0) {
                            updateProgress(bytesProcessedForLoad + bytesProcessedForPath, bytesToProcessForLoad);

                            updateMessage(String.format("Verified %,d concepts...", completionCount));
                        }
                        TtkConceptChronicle eConcept = new TtkConceptChronicle(dis);
                        if (watchSet.contains(eConcept.getPrimordialUuid())) {
                            System.out.println("Watch concept: " + eConcept);
                        }
                        conversionPermits.acquire();
                        conversionService.submit(new VerifyEConcept(termService, eConcept, conversionPermits, stampPath));

                        conceptCount++;

                        for (Future<Boolean> future = conversionService.poll(); future != null; future = conversionService.poll()) {
                                Boolean verified = future.get();
                                if (!verified) {
                                    filesVerified = false;
                                }
                                completionCount++;
                        }
                    }

                } catch (EOFException eof) {
                    // nothing to do.
                }

                bytesProcessedForLoad += bytesForPath;
                updateMessage("Verification of file: " + p.toFile().getName() + " complete, cleaning up converters.");
                while (completionCount < conceptCount) {
                    Future<Boolean> future = conversionService.take();
                        Boolean verified = future.get();
                        if (!verified) {
                            filesVerified = false;
                        }
                    completionCount++;

                }

            }
            Instant finish = Instant.now();
            Duration duration = Duration.between(start, finish);

            updateMessage("Verification of " + completionCount + " concepts complete in "
                    + duration.getSeconds() + " seconds.");
            updateProgress(bytesToProcessForLoad, bytesToProcessForLoad);
            return filesVerified;
        } finally {
            Hk2Looker.get().getService(ActiveTaskSet.class).get().remove(this);
        }
    }

    boolean isFxApplicationThread() {
        return Platform.isFxApplicationThread();
    }
}
