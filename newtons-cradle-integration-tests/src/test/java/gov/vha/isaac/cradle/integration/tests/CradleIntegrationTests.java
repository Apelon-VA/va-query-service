/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.integration.tests;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.cradle.taxonomy.graph.GraphCollector;
import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordPrimitive;
import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordUnpacked;
import gov.vha.isaac.cradle.taxonomy.walk.TaxonomyWalkAccumulator;
import gov.vha.isaac.cradle.taxonomy.walk.TaxonomyWalkCollector;
import gov.vha.isaac.cradle.waitfree.CasSequenceObjectMap;

import static gov.vha.isaac.lookup.constants.Constants.CHRONICLE_COLLECTIONS_ROOT_LOCATION_PROPERTY;

import gov.vha.isaac.metadata.coordinates.ViewCoordinates;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.ObjectChronicleTaskService;
import gov.vha.isaac.ochre.api.IdentifierService;
import gov.vha.isaac.ochre.api.coordinate.TaxonomyCoordinate;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import gov.vha.isaac.ochre.api.memory.HeapUseTicker;
import gov.vha.isaac.ochre.api.progress.ActiveTasksTicker;
import gov.vha.isaac.ochre.api.tree.TreeNodeVisitData;
import gov.vha.isaac.ochre.api.tree.hashtree.HashTreeBuilder;
import gov.vha.isaac.ochre.api.tree.hashtree.HashTreeWithBitSets;
import javafx.concurrent.Task;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.glassfish.hk2.api.MultiException;
import org.ihtsdo.otf.lookup.contracts.contracts.ActiveTaskSet;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
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

    public static void main(String[] args) {
        try {
            CradleIntegrationTests cit = new CradleIntegrationTests();
            cit.setUpSuite();
            cit.testLoad();
            cit.tearDownSuite();
        } catch (Exception ex) {
            log.fatal(ex.getLocalizedMessage(), ex);
        }
    }

    private static final Logger log = LogManager.getLogger();
    private boolean dbExists = false;
    private static IdentifierService sequenceProvider;

    @BeforeSuite
    public void setUpSuite() throws Exception {
        log.info("oneTimeSetUp");
        System.setProperty(CHRONICLE_COLLECTIONS_ROOT_LOCATION_PROPERTY, "target/object-chronicles");

        java.nio.file.Path dbFolderPath = Paths.get(System.getProperty(CHRONICLE_COLLECTIONS_ROOT_LOCATION_PROPERTY));
        dbExists = dbFolderPath.toFile().exists();
        System.out.println("termstore folder path: " + dbFolderPath.toFile().exists());

        LookupService.startupIsaac();
        sequenceProvider = Hk2Looker.getService(IdentifierService.class);
        ActiveTasksTicker.start(10);
        HeapUseTicker.start(10);
    }

    @AfterSuite
    public void tearDownSuite() throws Exception {
        log.info("oneTimeTearDown");
        LookupService.shutdownIsaac();
        ActiveTasksTicker.stop();
        HeapUseTicker.stop();
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
        ObjectChronicleTaskService tts = Hk2Looker.get().getService(ObjectChronicleTaskService.class);
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
            
             May want a white list or similar at some point.
             Assert.assertTrue(differences);
             */
        }
        testTaxonomy(mapDbService);

        walkTaxonomy(mapDbService);

        findRoots(mapDbService);

        HashTreeWithBitSets g = makeGraph(mapDbService);
        log.info("    taxonomy graph size:" + g.size());
        log.info("    taxonomy graph roots: " + Arrays.toString(g.getRootSequences()));
        for (int rootSequence : IntStream.of(g.getRootSequences()).limit(100).toArray()) {
            log.info("    rootSequence: " + rootSequence);
            ConceptChronicleBI aRoot = mapDbService.getConcept(rootSequence);
            log.info("    root concept: " + aRoot.toUserString());
            log.info("    parents of root" + Arrays.toString(g.getParentSequences(rootSequence)));
            TreeNodeVisitData dfsData = g.depthFirstProcess(rootSequence,
                    (nodeVisitData, sequence) -> {
                    });

            int maxDepth = dfsData.getMaxDepth();
            log.info(" Max depth from root (dfs): " + maxDepth);
            TreeNodeVisitData bfsData = g.breadthFirstProcess(rootSequence,
                    (nodeVisitData, sequence) -> {
                    });

            int maxBfsDepth = bfsData.getMaxDepth();
            log.info(" Max depth from root (bfs): " + maxBfsDepth);
            log.info("\n\n");
        }

        log.info("  Taxonomy graph leaves: " + g.getLeafSequences().length);
        log.info("  Taxonomy graph size: " + g.size());
        log.info("  Taxonomy graph concepts with parents count: " + g.conceptSequencesWithParentsCount());
        log.info("  Taxonomy graph concepts with children count: " + g.conceptSequencesWithChildrenCount());
    }

    private void findRoots(CradleExtensions cradle) {
        try {
            log.info("Walking 10 concepts to root inferred.");
            IntStream conceptSequenceStream = sequenceProvider.getConceptSequenceStream().limit(10);
            CasSequenceObjectMap<TaxonomyRecordPrimitive> taxonomyMap = cradle.getOriginDestinationTaxonomyMap();
            ViewCoordinate vc = ViewCoordinates.getDevelopmentInferredLatestActiveOnly();
            conceptSequenceStream.forEach((int conceptSequence) -> {
                walkToRoot(conceptSequence, taxonomyMap, vc, 0, new BitSet(), cradle);
                System.out.println("\n\n");
            });
            log.info("Walking 10 concepts to root stated.");
            conceptSequenceStream = sequenceProvider.getConceptSequenceStream().limit(10);
            CasSequenceObjectMap<TaxonomyRecordPrimitive> taxonomyMap2 = cradle.getOriginDestinationTaxonomyMap();
            ViewCoordinate vc2 = ViewCoordinates.getDevelopmentStatedLatestActiveOnly();
            conceptSequenceStream.forEach((int conceptSequence) -> {
                walkToRoot(conceptSequence, taxonomyMap2, vc2, 0, new BitSet(), cradle);
                System.out.println("\n\n");
            });
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

    }

    private void walkToRoot(int child, CasSequenceObjectMap<TaxonomyRecordPrimitive> taxonomyMap, TaxonomyCoordinate vp,
            int depth, BitSet visited, CradleExtensions cradle) {
        visited.set(child);
        if (depth > 50) {
            return;
        }
        try {
            ConceptChronicleBI childConcept = cradle.getConcept(child);
            java.util.Optional<TaxonomyRecordPrimitive> taxonomyRecord
                    = TaxonomyRecordPrimitive.getIfConceptActive(child, 
                            taxonomyMap, vp);

            if (taxonomyRecord.isPresent()) {

                StringBuilder sb = new StringBuilder();
                sb.append(childConcept.getNid());
                sb.append(":");
                sb.append(child);
                sb.append(" ");
                for (int i = 0; i < depth; i++) {
                    sb.append("  ");
                }
                sb.append(childConcept);
                System.out.println(sb.toString());
                TaxonomyRecordUnpacked record = taxonomyRecord.get().getTaxonomyRecordUnpacked();
                IntStream parentSequences = record.getActiveConceptSequencesForType(IsaacMetadataAuxiliaryBinding.IS_A.getSequence(), vp);
                parentSequences.forEach((int parentSequence) -> {
                    if (!visited.get(parentSequence)) {
                        walkToRoot(parentSequence, taxonomyMap, vp, depth + 1, visited, cradle);
                    }
                });
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void loadDatabase(ObjectChronicleTaskService tts, CradleExtensions ps) throws ExecutionException, IOException, MultiException, InterruptedException {
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

        log.info("  sequences map: {}", sequenceProvider.getConceptSequenceStream().distinct().count());
    }

    private boolean testLoad(ObjectChronicleTaskService tts, CradleExtensions ps) throws ExecutionException, IOException, MultiException, InterruptedException {
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
        log.info("  sequences map: {}", sequenceProvider.getConceptSequenceStream().distinct().count());

        return verified;
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

        System.out.println("Cornea is a " + vc.getRelationshipAssertionType() + " child-of disorder of eye: " + isChild);
        System.out.println("Cornea is a " + vc.getRelationshipAssertionType() + " kind-of of disorder of eye: " + isKind);
    }

    private void walkTaxonomy(CradleExtensions cradle) throws IOException {
        log.info("  Start walking taxonomy.");
        Instant collectStart = Instant.now();
        IntStream conceptSequenceStream = sequenceProvider.getParallelConceptSequenceStream();
        TaxonomyWalkCollector collector = new TaxonomyWalkCollector(cradle.getOriginDestinationTaxonomyMap(),
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

    private HashTreeWithBitSets makeGraph(CradleExtensions cradle) throws IOException {
        log.info("  Start to make taxonomy snapshot graph.");
        Instant collectStart = Instant.now();
        IntStream conceptSequenceStream = sequenceProvider.getParallelConceptSequenceStream();
        log.info("  conceptSequenceStream count 1:" + conceptSequenceStream.count());
        conceptSequenceStream = sequenceProvider.getParallelConceptSequenceStream();
        log.info("  conceptSequenceStream count 2:" + conceptSequenceStream.count());
        conceptSequenceStream = sequenceProvider.getParallelConceptSequenceStream();
        log.info("  conceptSequenceStream distinct count :" + conceptSequenceStream.distinct().count());
        conceptSequenceStream = sequenceProvider.getConceptSequenceStream();
        GraphCollector collector = new GraphCollector(cradle.getOriginDestinationTaxonomyMap(),
                ViewCoordinates.getDevelopmentInferredLatest());
        HashTreeBuilder graphBuilder = conceptSequenceStream.collect(
                HashTreeBuilder::new,
                collector,
                collector);
        HashTreeWithBitSets resultGraph = graphBuilder.getSimpleDirectedGraphGraph();
        Instant collectEnd = Instant.now();
        Duration collectDuration = Duration.between(collectStart, collectEnd);
        log.info("  Finished making graph: " + resultGraph);
        log.info("  Generation duration: " + collectDuration);
        return resultGraph;
    }

}
