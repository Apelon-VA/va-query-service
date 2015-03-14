/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.tasks;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.cradle.log.CradleCommitRecord;
import gov.vha.isaac.cradle.log.LogEntry;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import javafx.application.Platform;
import javafx.concurrent.Task;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ihtsdo.otf.lookup.contracts.contracts.ActiveTaskSet;
import org.ihtsdo.otf.tcc.dto.TtkConceptChronicle;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.ihtsdo.otf.tcc.model.cc.termstore.Termstore;

/**
 *
 * @author kec
 */
public class ImportCradleLogFile extends Task<Integer> {

    private static final Logger log = LogManager.getLogger();

    Path[] paths;
    CradleExtensions termService;

    public ImportCradleLogFile(Path[] paths, CradleExtensions termService) {
        updateTitle("Import Cradle Log");
        updateProgress(-1, Long.MAX_VALUE); // Indeterminate progress
        updateValue(0); // no concepts loaded
        this.paths = paths;
        this.termService = termService;
        Hk2Looker.get().getService(ActiveTaskSet.class).get().add(this);
    }

    @Override
    protected Integer call() throws Exception {
        try {
            Instant start = Instant.now();
            Semaphore conversionPermits = new Semaphore(Runtime.getRuntime().availableProcessors());
            ExecutorCompletionService conversionService = new ExecutorCompletionService(ForkJoinPool.commonPool());

            long bytesToProcessForLoad = 0;
            long bytesProcessedForLoad = 0;
            for (java.nio.file.Path p : paths) {
                bytesToProcessForLoad += p.toFile().length();
            }

            int entryCount = 0;
            int completionCount = 0;
            for (Path p : paths) {
                long bytesForPath = p.toFile().length();
                try (DataInputStream dis = new DataInputStream(new BufferedInputStream(Files.newInputStream(p)))) {
                    updateMessage("Importing log file: " + p.toFile().getName());
                    // allow for task cancellation, while still importing complete concepts.
                    while (!isCancelled()) {
                        long bytesProcessedForPath = bytesForPath - dis.available();
                        if (entryCount % 1000 == 0) {
                            updateProgress(bytesProcessedForLoad + bytesProcessedForPath, bytesToProcessForLoad);

                            updateMessage(String.format("Loaded %,d log entries...", completionCount));
                            updateValue(completionCount);
                        }

                        LogEntry logEntry = LogEntry.fromDataStream(dis);
                        switch (logEntry) {
                            case COMMIT_RECORD:
                                CradleCommitRecord ccr = new CradleCommitRecord(dis, (Termstore) termService);
                                conversionPermits.acquire();
                                conversionService.submit(new ImportCradleCommitRecord(ccr, conversionPermits));
                                break;
                            case CONCEPT:
                                TtkConceptChronicle eConcept = new TtkConceptChronicle(dis);
                                conversionPermits.acquire();
                                conversionService.submit(new ImportEConcept(eConcept, conversionPermits));
                                break;
                            default:
                                throw new UnsupportedOperationException("Can't handle: " + logEntry);
                        }

                        entryCount++;

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
                while (completionCount < entryCount) {
                    Future future = conversionService.take();
                    future.get();
                    completionCount++;
                }

            }
            Instant finish = Instant.now();
            Duration duration = Duration.between(start, finish);

            updateMessage("Load of " + completionCount + " log entries complete in "
                    + duration.getSeconds() + " seconds.");
            updateProgress(bytesToProcessForLoad, bytesToProcessForLoad);
            return entryCount;
        } finally {
            Hk2Looker.get().getService(ActiveTaskSet.class).get().remove(this);
        }
    }

    boolean isFxApplicationThread() {
        return Platform.isFxApplicationThread();
    }
}
