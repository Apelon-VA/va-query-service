package gov.vha.isaac.cradle.tasks;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.cradle.taxonomy.DestinationOriginRecord;
import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordPrimitive;
import gov.vha.isaac.cradle.waitfree.CasSequenceObjectMap;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.ConceptModel;
import gov.vha.isaac.ochre.api.ConceptProxy;
import gov.vha.isaac.ochre.api.ConfigurationService;
import gov.vha.isaac.ochre.api.Get;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.component.sememe.SememeBuilderService;
import gov.vha.isaac.ochre.api.logic.LogicalExpressionBuilderService;
import gov.vha.isaac.ochre.api.task.TimedTask;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import gov.vha.isaac.ochre.util.WorkExecutors;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import javafx.application.Platform;
import org.ihtsdo.otf.lookup.contracts.contracts.ActiveTaskSet;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;

/**
 * Created by kec on 7/22/14.
 */
public class ImportEConceptFile extends TimedTask<Integer> {

    private static final Logger log = LogManager.getLogger();
    // Below fields are to cache expensive operations once, so no need to repeat for
    // each import task, and not put in static to prevent data cache issues. 
    final CradleExtensions cradle = LookupService.getService(CradleExtensions.class);
    final SememeBuilderService sememeBuilderService = LookupService.getService(SememeBuilderService.class);
    final LogicalExpressionBuilderService expressionBuilderService
            = LookupService.getService(LogicalExpressionBuilderService.class);

    final int isaSequence;

    final CasSequenceObjectMap<TaxonomyRecordPrimitive> originDestinationTaxonomyRecords;
    final ConcurrentSkipListSet<DestinationOriginRecord> destinationOriginRecordSet;
    final ConceptSequenceSet neverRoleGroupConceptSequences = new ConceptSequenceSet();

    {
        originDestinationTaxonomyRecords = cradle.getOriginDestinationTaxonomyMap();
        destinationOriginRecordSet = cradle.getDestinationOriginRecordSet();
        isaSequence = Get.identifierService().getConceptSequenceForUuids(IsaacMetadataAuxiliaryBinding.IS_A.getUuids());
        neverRoleGroupConceptSequences.add(Get.identifierService().getConceptSequenceForUuids(Snomed.PART_OF.getUuids()));
        neverRoleGroupConceptSequences.add(Get.identifierService().getConceptSequenceForUuids(Snomed.LATERALITY.getUuids()));
        neverRoleGroupConceptSequences.add(Get.identifierService().getConceptSequenceForUuids(Snomed.HAS_ACTIVE_INGREDIENT.getUuids()));
        neverRoleGroupConceptSequences.add(Get.identifierService().getConceptSequenceForUuids(Snomed.HAS_DOSE_FORM.getUuids()));
    }
    // above fields are to cache expensive operations once for use by delegate tasks. 
    
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

    // http://stackoverflow.com/questions/26144594/reseting-classloader-of-a-maven-plugin

    // http://www.mograblog.com/2010/01/setting-classloader-in-maven-plugin.html
    @Override
    protected Integer call() throws Exception {
        try {
            Instant start = Instant.now();

            ExecutorCompletionService conversionService = new ExecutorCompletionService(LookupService.getService(WorkExecutors.class).getPotentiallyBlockingExecutor());

            AtomicLong bytesToProcessForLoad = new AtomicLong();
            AtomicLong bytesProcessedForLoad = new AtomicLong();
            for (java.nio.file.Path p : paths) {
                bytesToProcessForLoad.addAndGet(p.toFile().length());
                // a second pass for conversion. 
                bytesToProcessForLoad.addAndGet(p.toFile().length());
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
                            (TtkConceptChronicle eConcept) -> new ConvertOtfToOchreModel(eConcept, stampPathUuid, this));
                    break;
                case OTF_CONCEPT_MODEL:
                    doImport(conceptCount, bytesProcessedForLoad, bytesToProcessForLoad, completionCount, conversionService,
                            "loaded",
                            (TtkConceptChronicle eConcept) -> new ImportEConceptOtfModel(eConcept, stampPathUuid));
                    doImport(conceptCount, bytesProcessedForLoad, bytesToProcessForLoad, completionCount, conversionService,
                            "converted",
                            (TtkConceptChronicle eConcept) -> new ConvertOtfToOchreModel(eConcept, stampPathUuid, this));
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
                setProgressMessageGenerator((task) -> {
                    updateMessage(String.format("Importing file: " + p.toFile().getName()
                            + " " + actionForMessage + " %,d concepts...", completionCount.get()));
                    updateValue(completionCount.get());
                    try {
                        updateProgress(bytesProcessedForLoad.get() + bytesForPath - dis.available(), bytesToProcessForLoad.get());
                    } catch (IOException ex) {
                       updateProgress(bytesProcessedForLoad.get(), bytesToProcessForLoad.get());
                    }
                });
                // allow for task cancellation, while still importing complete concepts.
                while (!isCancelled()) {
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
            setProgressMessageGenerator((task) -> {
                updateProgress(bytesProcessedForLoad.get(), bytesToProcessForLoad.get());
                updateMessage("Importing of file: " + p.toFile().getName() + " complete, cleaning up converters.");
                updateValue(completionCount.get());
            });

            bytesProcessedForLoad.addAndGet(bytesForPath);
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
