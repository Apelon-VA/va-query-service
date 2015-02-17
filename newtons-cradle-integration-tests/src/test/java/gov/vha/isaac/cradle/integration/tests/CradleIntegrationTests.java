/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.integration.tests;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.cradle.collections.CasSequenceObjectMap;
import gov.vha.isaac.cradle.taxonomy.GraphCollector;
import gov.vha.isaac.cradle.taxonomy.PrimitiveTaxonomyRecord;
import gov.vha.isaac.cradle.taxonomy.TaxonomyAccumulator;
import gov.vha.isaac.cradle.taxonomy.TaxonomyCollector;
import gov.vha.isaac.cradle.taxonomy.TaxonomyFlags;
import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordUnpacked;
import gov.vha.isaac.cradle.taxonomy.TaxonomyWalkAccumulator;
import gov.vha.isaac.cradle.taxonomy.TaxonomyWalkCollector;
import gov.vha.isaac.cradle.version.ViewPoint;
import static gov.vha.isaac.lookup.constants.Constants.CHRONICLE_COLLECTIONS_ROOT_LOCATION_PROPERTY;

import gov.vha.isaac.metadata.coordinates.ViewCoordinates;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.ObjectChronicleTaskServer;
import gov.vha.isaac.ochre.api.graph.SimpleDirectedGraph;
import gov.vha.isaac.ochre.api.graph.SimpleDirectedGraphBuilder;
import gov.vha.isaac.ochre.api.graph.SimpleDirectedGraphVisitData;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import javafx.concurrent.Task;
import javafx.embed.swing.JFXPanel;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.mahout.math.set.OpenIntHashSet;
import org.glassfish.hk2.api.MultiException;
import org.glassfish.hk2.runlevel.RunLevelController;
import org.ihtsdo.otf.lookup.contracts.contracts.ActiveTaskSet;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.api.coordinate.Precedence;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.ihtsdo.otf.tcc.model.cc.termstore.PersistentStoreI;
import org.jvnet.testing.hk2testng.HK2;
import org.reactfx.EventStreams;
import org.reactfx.Subscription;
import org.testng.annotations.*;


/**
 *
 * @author kec
 */

// https://www.jfokus.se/jfokus08/pres/jf08-HundredKilobytesKernelHK2.pdf
// https://github.com/saden1/hk2-testng
@HK2("cradle")
public class CradleIntegrationTests {
    
    private static Logger log = LogManager.getLogger();
    Subscription tickSubscription;
    RunLevelController runLevelController;
    private boolean dbExists = false;
    

    @BeforeSuite
    public void setUpSuite() throws Exception {
        log.info("oneTimeSetUp");
        JFXPanel panel = new JFXPanel();
        System.setProperty(CHRONICLE_COLLECTIONS_ROOT_LOCATION_PROPERTY, "target/object-chronicles");

        java.nio.file.Path dbFolderPath = Paths.get(System.getProperty(CHRONICLE_COLLECTIONS_ROOT_LOCATION_PROPERTY));
        dbExists = dbFolderPath.toFile().exists();
        System.out.println("termstore folder path: " + dbFolderPath.toFile().exists());
        
        runLevelController = Hk2Looker.get().getService(RunLevelController.class);

        log.info("going to run level 1");
        runLevelController.proceedTo(1);
        log.info("going to run level 2");
        runLevelController.proceedTo(2);
        tickSubscription = EventStreams.ticks(Duration.ofSeconds(10))
                .subscribe(tick -> {
                    Set<Task> taskSet = Hk2Looker.get().getService(ActiveTaskSet.class).get();
                    taskSet.stream().forEach((task) -> {
                        log.printf(Level.INFO, "%n    %s%n    %s%n    %.1f%% complete",
                                task.getTitle(), task.getMessage(), task.getProgress() * 100);
            });
                });
    }

    @AfterSuite
    public void tearDownSuite() throws Exception {
        log.info("oneTimeTearDown");
        log.info("going to run level 1");
        runLevelController.proceedTo(1);
        log.info("going to run level 0");
        runLevelController.proceedTo(0);
        tickSubscription.unsubscribe();
    }
    
    @BeforeMethod
    public void setUp() throws Exception {

    }

    @AfterMethod
    public void tearDown() throws Exception {

    }
    
    @Test
    public void testLoad() throws Exception {

        log.info("  Testing load...");
        ObjectChronicleTaskServer tts = Hk2Looker.get().getService(ObjectChronicleTaskServer.class);
        PersistentStoreI ps = Hk2Looker.get().getService(PersistentStoreI.class);

        String mapDbFolder = System.getProperty(CHRONICLE_COLLECTIONS_ROOT_LOCATION_PROPERTY);
        if (mapDbFolder == null || mapDbFolder.isEmpty()) {
            throw new IllegalStateException(CHRONICLE_COLLECTIONS_ROOT_LOCATION_PROPERTY + " has not been set.");
        }

        CradleExtensions mapDbService = (CradleExtensions) ps;

        if (!dbExists) {
            loadDatabase(tts, mapDbService);
            boolean differences = testLoad(tts, mapDbService);
            
            /*
                    There will be some differences as importing change sets or the 
                    path eConcept file may have multiple incremental entries for 
                    a single concept. 
            
                    May want a white list or somilar at some point. 
                    Assert.assertTrue(differences);
            */
        }
        testTaxonomy(mapDbService);

        walkTaxonomy(mapDbService);

        findRoots(mapDbService);


        SimpleDirectedGraph g = makeGraph(mapDbService);
        log.info("    graph size:" + g.size());
        log.info("    Graph roots: " + Arrays.toString(g.getRootSequences()));
        for (int rootSequence : IntStream.of(g.getRootSequences()).limit(10).toArray()) {
            log.info("    rootSequence: " + rootSequence);
            log.info("    root concept: " + mapDbService.getConcept(rootSequence).toUserString());
            log.info("    parents of root" + Arrays.toString(g.getParents(rootSequence)));
            SimpleDirectedGraphVisitData dfsData = g.depthFirstSearch(rootSequence);
            log.info("  dfsData:" + dfsData);

            OptionalDouble averageDepth = dfsData.getAverageDepth();
            if (averageDepth.isPresent()) {
                log.info(" Average depth from root: " + averageDepth.getAsDouble());
            }
            int maxDepth = dfsData.getMaxDepth();
            log.info(" Map depth from root: " + maxDepth);
            SimpleDirectedGraphVisitData bfsData = g.breadthFirstSearch(rootSequence);
            log.info("  bfsData:" + bfsData);

            OptionalDouble averageBfsDepth = bfsData.getAverageDepth();
            if (averageBfsDepth.isPresent()) {
                log.info(" Average depth from root: " + averageBfsDepth.getAsDouble());
            }
            int maxBfsDepth = bfsData.getMaxDepth();
            log.info(" Map depth from root: " + maxBfsDepth);
            log.info("\n\n");
        }

        log.info("  Graph leaves: " + g.getLeaves().length);
        log.info("  Graph size: " + g.size());
        log.info("  Graph concepts with parents count: " + g.conceptSequencesWithParentsCount());
        log.info("  Graph concepts with children count: " + g.conceptSequencesWithChildrenCount());
    }
    
    private void findRoots(CradleExtensions cradle) {
        try {
            IntStream conceptSequenceStream = cradle.getConceptSequenceStream().limit(10);
            CasSequenceObjectMap<PrimitiveTaxonomyRecord> taxonomyMap = cradle.getTaxonomyMap();
            ViewCoordinate vc = ViewCoordinates.getDevelopmentInferredLatest();
            EnumSet<TaxonomyFlags> flags = TaxonomyFlags.INFERRED_PARENT_FLAGS_SET;
            OpenIntHashSet activeModuleNids = new OpenIntHashSet();  // null or empty is a wild card 
            ViewPoint vp = new ViewPoint(vc.getViewPosition(), activeModuleNids, Precedence.PATH);
            conceptSequenceStream.forEach((int conceptSequence) -> {
                walkToRoot(conceptSequence, taxonomyMap, vp, flags, 0, new BitSet(), cradle);
                System.out.println("\n\n");
            });
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

    }

    private void walkToRoot(int child, CasSequenceObjectMap<PrimitiveTaxonomyRecord> taxonomyMap, ViewPoint vp,
            EnumSet<TaxonomyFlags> flags, int depth, BitSet visited, CradleExtensions cradle) {
        visited.set(child);
        if (depth > 25) {
            return;
        }
        try {
            ConceptChronicleBI childConcept = cradle.getConcept(child);
            java.util.Optional<PrimitiveTaxonomyRecord> taxonomyRecord = taxonomyMap.get(child);

            StringBuilder sb = new StringBuilder();
            sb.append(childConcept.getNid());
            sb.append(":");
            sb.append(child);
            sb.append(" ");
            for (int i = 0; i < depth; i++) {
                sb.append("  ");
            }
            sb.append(childConcept);
//            if (taxonomyRecord.isPresent()) {
//                sb.append(" ");
//                sb.append(taxonomyRecord.get());
//            }
            System.out.println(sb.toString());
            if (taxonomyRecord.isPresent()) {
                TaxonomyRecordUnpacked record = taxonomyRecord.get().getTaxonomyRecordUnpacked();
                IntStream parentSequences = record.getActiveConceptSequences(flags, vp);
                parentSequences.forEach((int parentSequence) -> {
                    if (!visited.get(parentSequence)) {
                        walkToRoot(parentSequence, taxonomyMap, vp, flags, depth + 1, visited, cradle);
                    }
                });

            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

    }

    private void loadDatabase(ObjectChronicleTaskServer tts, CradleExtensions ps) throws ExecutionException, IOException, MultiException, InterruptedException {
        Path snomedDataFile = Paths.get("target/data/sctSiEConcepts.jbin");
        Path isaacMetadataFile = Paths.get("target/data/isaac/metadata/econ/IsaacMetadataAuxiliary.econ");

        Instant start = Instant.now();

        Task<Integer> loadTask = tts.startLoadTask(IsaacMetadataAuxiliaryBinding.DEVELOPMENT,
                isaacMetadataFile, snomedDataFile);
        Hk2Looker.get().getService(ActiveTaskSet.class).get().add(loadTask);
        int conceptCount = loadTask.get();
        Hk2Looker.get().getService(ActiveTaskSet.class).get().remove(loadTask);
        Instant finish = Instant.now();
        Duration duration = Duration.between(start, finish);
        log.info("  Loaded " + conceptCount + " concepts in: " + duration);
        double nsPerConcept = 1.0d * duration.toNanos() / conceptCount;
        log.info("  nsPerConcept: {}", nsPerConcept);

        double msPerConcept = 1.0d * duration.toMillis() / conceptCount;
        log.info("  msPerConcept: {}", msPerConcept);

        log.info("  concepts in map: {}", ps.getConceptCount());

        log.info("  sequences map: {}", ps.getConceptSequenceStream().distinct().count());
    }
    
    private boolean testLoad(ObjectChronicleTaskServer tts, CradleExtensions ps) throws ExecutionException, IOException, MultiException, InterruptedException {
        Path snomedDataFile = Paths.get("target/data/sctSiEConcepts.jbin");
        Path isaacMetadataFile = Paths.get("target/data/isaac/metadata/econ/IsaacMetadataAuxiliary.econ");
        Instant start = Instant.now();

        Task<Boolean> verifyTask = tts.startVerifyTask(IsaacMetadataAuxiliaryBinding.DEVELOPMENT,
                isaacMetadataFile, snomedDataFile);
        Hk2Looker.get().getService(ActiveTaskSet.class).get().add(verifyTask);
        boolean verified = verifyTask.get();
        Hk2Looker.get().getService(ActiveTaskSet.class).get().remove(verifyTask);
        Instant finish = Instant.now();
        Duration duration = Duration.between(start, finish);
        log.info("  Verified concepts in: " + duration);
        log.info("  concepts in map: {}", ps.getConceptCount());
        log.info("  sequences map: {}", ps.getConceptSequenceStream().distinct().count());

        return verified;
    }
    

    private void makeTaxonomyRecords(CradleExtensions mapDbService) throws IOException {
        Instant collectStart = Instant.now();

        log.info("  ConceptDataEagerStream count: " + mapDbService.getParallelConceptDataEagerStream().count());

        TaxonomyAccumulator statistics = mapDbService.getParallelConceptDataEagerStream().collect(new TaxonomyCollector());
        //TaxonomyAccumulator statistics = mapDbService.getConceptDataEagerStream().collect(new TaxonomyCollector());
        Instant collectEnd = Instant.now();
        Duration collectDuration = Duration.between(collectStart, collectEnd);
        log.info("  DataEager Parallel stats: " + statistics);
        //log.info("  DataEager sequential stats: " + statistics);
        log.info("  Collection duration: " + collectDuration);

    }
    
    private void testTaxonomy(CradleExtensions cradle) {
        try {
            testTaxonomy(cradle, ViewCoordinates.getDevelopmentInferredLatest());
            testTaxonomy(cradle, ViewCoordinates.getDevelopmentStatedLatest());
        } catch (IOException | ContradictionException ex) {
            log.error(ex.getLocalizedMessage(), ex);
        }

    }

    private void testTaxonomy(CradleExtensions cradle, ViewCoordinate vc) throws IOException, ContradictionException {
        int disorderOfCorneaNid = Snomed.DISORDER_OF_CORNEA.getNid();
        int disorderOfEyeNid = Snomed.DISORDER_OF_EYE.getNid();
        boolean isChild = cradle.isChildOf(disorderOfCorneaNid, disorderOfEyeNid, vc);
        boolean isKind = cradle.isKindOf(disorderOfCorneaNid, disorderOfEyeNid, vc);

        System.out.println("Cornea is a " + vc.getRelationshipAssertionType() + " child of disorder of eye: " + isChild);
        System.out.println("Cornea is a " + vc.getRelationshipAssertionType() + " kind-of of disorder of eye: " + isKind);
    }

    private void walkTaxonomy(CradleExtensions cradle) throws IOException {
        log.info("  Start walking taxonomy.");
        Instant collectStart = Instant.now();
        IntStream conceptSequenceStream = cradle.getParallelConceptSequenceStream();
        TaxonomyWalkCollector collector = new TaxonomyWalkCollector(cradle.getTaxonomyMap(),
                ViewCoordinates.getDevelopmentStatedLatest());
        TaxonomyWalkAccumulator taxonomyWalkAccumulator = conceptSequenceStream.collect(
                TaxonomyWalkAccumulator::new,
                collector,
                collector);
        Instant collectEnd = Instant.now();
        Duration collectDuration = Duration.between(collectStart, collectEnd);
        log.info("  Finished walking taxonomy: " + taxonomyWalkAccumulator);
        log.info("  Collection duration: " + collectDuration);
    }

    private SimpleDirectedGraph makeGraph(CradleExtensions cradle) throws IOException {
        log.info("  Start to make graph.");
        Instant collectStart = Instant.now();
        IntStream conceptSequenceStream = cradle.getParallelConceptSequenceStream();
        log.info("  conceptSequenceStream count 1:" + conceptSequenceStream.count());
        conceptSequenceStream = cradle.getParallelConceptSequenceStream();
        log.info("  conceptSequenceStream count 2:" + conceptSequenceStream.count());
        conceptSequenceStream = cradle.getParallelConceptSequenceStream();
        log.info("  conceptSequenceStream distinct count :" + conceptSequenceStream.distinct().count());
        conceptSequenceStream = cradle.getConceptSequenceStream();
        GraphCollector collector = new GraphCollector(cradle.getTaxonomyMap(),
                ViewCoordinates.getDevelopmentInferredLatest());
        SimpleDirectedGraphBuilder graphBuilder = conceptSequenceStream.collect(
                SimpleDirectedGraphBuilder::new,
                collector,
                collector);
        SimpleDirectedGraph resultGraph = graphBuilder.getSimpleDirectedGraphGraph();
        Instant collectEnd = Instant.now();
        Duration collectDuration = Duration.between(collectStart, collectEnd);
        log.info("  Finished making graph: " + resultGraph);
        log.info("  Generation duration: " + collectDuration);
        return resultGraph;
    }

}
