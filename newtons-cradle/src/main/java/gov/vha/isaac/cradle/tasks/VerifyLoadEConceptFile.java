/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.tasks;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.ochre.api.ConceptModel;
import gov.vha.isaac.ochre.api.ConceptProxy;
import gov.vha.isaac.ochre.api.ConfigurationService;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.task.TimedTask;
import gov.vha.isaac.ochre.util.WorkExecutors;
import javafx.application.Platform;
import javafx.concurrent.Task;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ihtsdo.otf.lookup.contracts.contracts.ActiveTaskSet;
import org.ihtsdo.otf.tcc.dto.TtkConceptChronicle;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;

/**
 *
 * @author kec
 */
public class VerifyLoadEConceptFile
        extends TimedTask<Boolean> {

    private static final Logger log = LogManager.getLogger();
    private static final Set<UUID> watchSet = new HashSet<>();
//            new HashSet<>(Arrays.asList(UUID.fromString("bcefc7ae-7512-3893-ade1-8eae817b4f0d"), 
//            UUID.fromString("afee3454-a2ef-3d89-a51e-c3337317296a")));

    private static final Set<UUID> whiteList = new HashSet<>(Arrays.asList(
            UUID.fromString("ee9ac5d2-a07c-3981-a57a-f7f26baf38d8"), // health concept

            UUID.fromString("3b0dbd3b-2e53-3a30-8576-6c7fa7773060"), // stated
            UUID.fromString("00791270-77c9-32b6-b34f-d932569bd2bf"), // fsn
            UUID.fromString("1290e6ba-48d0-31d2-8d62-e133373c63f5"), // inferred
            UUID.fromString("8bfba944-3965-3946-9bcb-1e80a5da63a2"), // synonym
            UUID.fromString("700546a3-09c7-3fc2-9eb9-53d318659a09"), // definition
            UUID.fromString("12b9e103-060e-3256-9982-18c1191af60e"), // acceptable
            UUID.fromString("266f1bc3-3361-39f3-bffe-69db9daea56e"), // preferred
            UUID.fromString("c93a30b9-ba77-3adb-a9b8-4589c9f8fb25"), // is-a
            UUID.fromString("6155818b-09ed-388e-82ce-caa143423e99"), // role
            UUID.fromString("eb9a5e42-3cba-356d-b623-3ed472e20b30"), // GB Dialect
            UUID.fromString("bca0a686-3516-3daf-8fcf-fe396d13cfad") // US dialect

    ));

    Path[] paths;
    CradleExtensions termService;
    ConceptProxy stampPath = null;
    ConceptModel conceptModel;

    private VerifyLoadEConceptFile(Path[] paths, CradleExtensions termService) {
        updateTitle("Verify load of concept files");
        updateProgress(-1, Long.MAX_VALUE); // Indeterminate progress
        this.paths = paths;
        this.termService = termService;
        this.conceptModel = LookupService.getService(ConfigurationService.class).getConceptModel();
    }

    private VerifyLoadEConceptFile(Path[] paths, CradleExtensions termService, ConceptProxy stampPath) {
        this(paths, termService);
        this.stampPath = stampPath;
    }

    public static VerifyLoadEConceptFile create(Path[] paths, CradleExtensions termService) {
        VerifyLoadEConceptFile verifyLoad = new VerifyLoadEConceptFile(paths, termService);
        LookupService.getService(ActiveTaskSet.class).get().add(verifyLoad);
        return verifyLoad;
    }

    public static VerifyLoadEConceptFile create(Path[] paths, CradleExtensions termService, ConceptProxy stampPath) {
        VerifyLoadEConceptFile verifyLoad = new VerifyLoadEConceptFile(paths, termService, stampPath);
        LookupService.getService(ActiveTaskSet.class).get().add(verifyLoad);
        return verifyLoad;
    }

    @Override
    protected Boolean call() throws Exception {
        boolean filesVerified = true;
        try {
            Instant start = Instant.now();
            ExecutorCompletionService<Boolean> conversionService = new ExecutorCompletionService(LookupService.getService(WorkExecutors.class).getPotentiallyBlockingExecutor());

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
//                        if (watchSet.contains(eConcept.getPrimordialUuid())) {
//                            System.out.println("Watch concept: " + eConcept);
//                        }
                        if (!whiteList.contains(eConcept.getPrimordialUuid())) {
                            switch (conceptModel) {
                                case OCHRE_CONCEPT_MODEL:
                                    conversionService.submit(new VerifyEConceptOchreModel(termService, eConcept, stampPath));
                                    break;
                                case OTF_CONCEPT_MODEL:
                                    conversionService.submit(new VerifyEConceptOtfModel(termService, eConcept, stampPath));
                                    break;
                                default:
                                    throw new UnsupportedOperationException("Can't handle: " + conceptModel);
                            }
                            conceptCount++;
                        }

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
            LookupService.getService(ActiveTaskSet.class).get().remove(this);
        }
    }

    boolean isFxApplicationThread() {
        return Platform.isFxApplicationThread();
    }
}
