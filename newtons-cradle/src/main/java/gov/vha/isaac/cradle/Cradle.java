package gov.vha.isaac.cradle;

import gov.vha.isaac.cradle.refex.RefexCollection;
import gov.vha.isaac.cradle.sememe.SememeKey;
import gov.vha.isaac.cradle.tasks.*;
import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordPrimitive;
import gov.vha.isaac.cradle.waitfree.CasSequenceObjectMap;
import gov.vha.isaac.cradle.collections.ConcurrentSequenceSerializedObjectMap;
import gov.vha.isaac.cradle.collections.UuidIntMapMap;
import gov.vha.isaac.cradle.component.ConceptChronicleDataEager;
import gov.vha.isaac.cradle.component.ConceptChronicleDataEagerSerializer;
import org.ihtsdo.otf.tcc.model.cc.refex.RefexService;
import gov.vha.isaac.cradle.taxonomy.DestinationOriginRecord;
import gov.vha.isaac.cradle.taxonomy.CradleTaxonomyProvider;
import gov.vha.isaac.metadata.coordinates.ViewCoordinates;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.ConceptProxy;
import gov.vha.isaac.ochre.api.LookupService;
import javafx.concurrent.Task;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.glassfish.hk2.runlevel.RunLevel;
import org.ihtsdo.otf.tcc.api.blueprint.TerminologyBuilderBI;
import org.ihtsdo.otf.tcc.api.conattr.ConceptAttributeVersionBI;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.api.concept.ConceptVersionBI;
import org.ihtsdo.otf.tcc.api.concept.ProcessUnfetchedConceptDataBI;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.api.coordinate.EditCoordinate;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.cs.ChangeSetPolicy;
import org.ihtsdo.otf.tcc.api.cs.ChangeSetWriterThreading;
import org.ihtsdo.otf.tcc.api.db.DbDependency;
import org.ihtsdo.otf.tcc.api.description.DescriptionVersionBI;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.tcc.api.nid.NidSetBI;
import org.ihtsdo.otf.tcc.api.refex.RefexChronicleBI;
import org.ihtsdo.otf.tcc.api.relationship.RelationshipVersionBI;
import org.ihtsdo.otf.tcc.ddo.ComponentReference;
import org.ihtsdo.otf.tcc.ddo.concept.ConceptChronicleDdo;
import org.ihtsdo.otf.tcc.ddo.fetchpolicy.RefexPolicy;
import org.ihtsdo.otf.tcc.ddo.fetchpolicy.RelationshipPolicy;
import org.ihtsdo.otf.tcc.ddo.fetchpolicy.VersionPolicy;
import org.ihtsdo.otf.tcc.model.cc.NidPairForRefex;
import org.ihtsdo.otf.tcc.model.cc.concept.ConceptChronicle;
import org.ihtsdo.otf.tcc.model.cc.refex.RefexMember;
import org.ihtsdo.otf.tcc.model.cc.relationship.Relationship;
import org.ihtsdo.otf.tcc.model.cc.termstore.Termstore;
import org.jvnet.hk2.annotations.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.beans.PropertyChangeListener;
import java.beans.VetoableChangeListener;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import gov.vha.isaac.ochre.api.ObjectChronicleTaskService;
import gov.vha.isaac.ochre.api.SequenceService;
import gov.vha.isaac.ochre.api.StandardPaths;
import gov.vha.isaac.ochre.api.TaxonomyService;
import gov.vha.isaac.ochre.api.commit.CommitManager;
import gov.vha.isaac.ochre.api.sememe.SememeService;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import gov.vha.isaac.ochre.collections.NidSet;
import java.nio.file.Path;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.ihtsdo.otf.tcc.api.chronicle.ComponentChronicleBI;
import org.ihtsdo.otf.tcc.api.concept.ConceptFetcherBI;
import org.ihtsdo.otf.tcc.api.nid.ConcurrentBitSet;
import org.ihtsdo.otf.tcc.api.nid.IntSet;
import org.ihtsdo.otf.tcc.dto.TtkConceptChronicle;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.ihtsdo.otf.tcc.model.cc.component.ConceptComponent;

/**
 * Created by kec on 12/18/14.
 */
@Service(name = "ChRonicled Assertion Database of Logical Expressions")
@RunLevel(value = 1)
public class Cradle
        extends Termstore
        implements ObjectChronicleTaskService, CradleExtensions {

    public static final String DEFAULT_CRADLE_FOLDER = "cradle";
    private static final Logger log = LogManager.getLogger();

    final UuidIntMapMap uuidIntMap = new UuidIntMapMap();

    final ConcurrentSequenceSerializedObjectMap<ConceptChronicleDataEager> conceptMap;

    final ConcurrentSequenceIntMap nidCnidMap = new ConcurrentSequenceIntMap();


    transient HashMap<UUID, ViewCoordinate> viewCoordinates = new HashMap<>();


    AtomicBoolean loadExisting = new AtomicBoolean(false);

    SequenceService sequenceProvider;
    TaxonomyService taxonomyProvider;
    CommitManager commitManager;
    SememeService sememeProvider;
    RefexService refexProvider;

    Cradle() throws IOException, NumberFormatException, ParseException {
        conceptMap = new ConcurrentSequenceSerializedObjectMap(new ConceptChronicleDataEagerSerializer(),
                IsaacDbFolder.get().getDbFolderPath(), "concept-map/", ".concepts.map");

    }

    @PostConstruct
    private void startMe() throws IOException {
        log.info("Starting Cradle post-construct");
        commitManager = LookupService.getService(CommitManager.class);
        sequenceProvider = LookupService.getService(SequenceService.class);
        sememeProvider = LookupService.getService(SememeService.class);
        refexProvider = LookupService.getService(RefexService.class);
        if (!IsaacDbFolder.get().getPrimordial()) {
            loadExisting.set(!IsaacDbFolder.get().getPrimordial());
        }
    }

    @Override
    public void loadExistingDatabase() throws IOException {
        taxonomyProvider = Hk2Looker.getService(TaxonomyService.class);
        if (loadExisting.compareAndSet(true, false)) {

            log.info("Loading sequence-cnid-map.");
            nidCnidMap.read(new File(IsaacDbFolder.get().getDbFolderPath().toFile(), "sequence-cnid-map"));

            log.info("Loading concept-map.");
            conceptMap.read();
            log.info("Loading uuid-nid-map.");
            uuidIntMap.read(new File(IsaacDbFolder.get().getDbFolderPath().toFile(), "uuid-nid-map"));

            log.info("Finished load.");
        }

    }

    @PreDestroy
    private void stopMe() throws IOException {
        log.info("Stopping Cradle pre-destroy. ");

        log.info("writing concept-map.");
        conceptMap.write();

        log.info("writing uuid-nid-map.");
        uuidIntMap.write(new File(IsaacDbFolder.get().getDbFolderPath().toFile(), "uuid-nid-map"));

        log.info("writing sequence-cnid-map.");
        //integrityTest();
        nidCnidMap.write(new File(IsaacDbFolder.get().getDbFolderPath().toFile(), "sequence-cnid-map"));

        log.info("Finished pre-destroy.");
    }

    public void integrityTest() {
        log.info("NidCnid integrity test. ");
        IntStream componentsNotSet = nidCnidMap.getComponentsNotSet();
        NidSet componentNidsNotSet
                = NidSet.of(componentsNotSet.map((nid) -> {
                    return nid - Integer.MIN_VALUE;
                }).toArray());
        componentNidsNotSet.remove(Integer.MIN_VALUE); // we know Integer.MIN_VALUE is not used. 
        log.info("Components with no concept: " + componentNidsNotSet.size());
        getParallelConceptDataEagerStream().forEach((ConceptChronicleDataEager cde) -> {
            if (componentNidsNotSet.contains(cde.getNid())) {
                if (nidCnidMap.containsKey(cde.getNid())) {
                    int key = nidCnidMap.get(cde.getNid()).getAsInt();
                    System.out.println("Concept in nidCnidMap, but not componentsNotSet: " + cde);
                    componentNidsNotSet.contains(cde.getNid());
                } else {
                    System.out.println("Concept not in nidCnidMap: " + cde);
                }
            }
            cde.getConceptComponents().forEach((ConceptComponent<?, ?> cc) -> {
                if (componentNidsNotSet.contains(cc.getNid())) {
                    if (nidCnidMap.containsKey(cde.getNid())) {
                        int key = nidCnidMap.get(cde.getNid()).getAsInt();
                        System.out.println("component in nidCnidMap, but not componentsNotSet: " + cc);
                        componentNidsNotSet.contains(cde.getNid());
                    } else {
                        System.out.println("component not in nidCnidMap: " + cc);
                    }
                }
            });
        });

        getParallelRefexStream().forEach((RefexMember<?, ?> sememe) -> {
            if (componentNidsNotSet.contains(sememe.getNid())) {
                if (nidCnidMap.containsKey(sememe.getNid())) {
                    int key = nidCnidMap.get(sememe.getNid()).getAsInt();
                    System.out.println("Sememe in nidCnidMap, but not componentsNotSet: " + sememe);
                    componentNidsNotSet.contains(sememe.getNid());
                } else {
                    System.out.println("Sememe not in nidCnidMap: " + sememe);
                }
            }
        });

        componentNidsNotSet.stream().limit(100).forEach((int nid) -> {
            try {
                List<UUID> uuids = getUuidsForNid(nid);
                System.out.println("Unmapped nid: " + nid + " UUIDs:" + uuids);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    @Override
    public void writeRefex(RefexMember<?, ?> refex) {
        refexProvider.writeRefex(refex);
    }

    @Override
    public Stream<RefexMember<?, ?>> getRefexStream() {
        return refexProvider.getRefexStream();
    }

    @Override
    public Stream<RefexMember<?, ?>> getParallelRefexStream() {
        return refexProvider.getParallelRefexStream();
    }

    @Override
    public Stream<ConceptChronicleDataEager> getConceptDataEagerStream() {
        return conceptMap.getStream();
    }

    @Override
    public Stream<ConceptChronicleDataEager> getConceptDataEagerStream(ConceptSequenceSet conceptSequences) {
        return sequenceProvider.getConceptSequenceStream()
                .filter((int sequence) -> conceptSequences.contains(sequence))
                .mapToObj((int sequence) -> {
                    Optional<ConceptChronicleDataEager> result = conceptMap.get(sequence);
                    if (result.isPresent()) {
                        return conceptMap.get(sequence).get();
                    }
                    throw new IllegalStateException("No concept for sequence: " + sequence);
                });

    }

    @Override
    public Stream<ConceptChronicleDataEager> getParallelConceptDataEagerStream(ConceptSequenceSet conceptSequences) {
        return sequenceProvider.getParallelConceptSequenceStream()
                .filter((int sequence) -> conceptSequences.contains(sequence))
                .mapToObj((int sequence) -> {
                    Optional<ConceptChronicleDataEager> result = conceptMap.get(sequence);
                    if (result.isPresent()) {
                        return conceptMap.get(sequence).get();
                    }
                    throw new IllegalStateException("No concept for sequence: " + sequence);
                });
    }

    @Override
    public Stream<ConceptChronicleDataEager> getParallelConceptDataEagerStream() {
        return conceptMap.getParallelStream();
    }

    @Override
    public Stream<ConceptChronicle> getConceptStream() {
        return conceptMap.getStream().map(ConceptChronicleDataEager::getConceptChronicle);
    }

    @Override
    public Stream<ConceptChronicle> getParallelConceptStream() {
        return conceptMap.getParallelStream().map(ConceptChronicleDataEager::getConceptChronicle);
    }

    @Override
    public Stream<? extends ConceptChronicleBI> getConceptStream(ConceptSequenceSet conceptSequences) throws IOException {
        return conceptSequences.stream().mapToObj((int sequence) -> conceptMap.getQuick(sequence).getConceptChronicle());
    }

    @Override
    public Stream<? extends ConceptChronicleBI> getParallelConceptStream(ConceptSequenceSet conceptSequences) throws IOException {
        return conceptSequences.stream().parallel().mapToObj((int sequence) -> conceptMap.getQuick(sequence).getConceptChronicle());
    }

    /**
     * For debugging...
     */
    private static HashSet<UUID> watchSet = new HashSet<>();

    {
//        watchSet.add(UUID.fromString("0418a591-f75b-39ad-be2c-3ab849326da9"));
//        watchSet.add(UUID.fromString("4459d8cf-5a6f-3952-9458-6d64324b27b7"));
    }

    @Override
    public int getNidForUuids(UUID... uuids) throws IOException {

        for (UUID uuid : uuids) {
//            if (watchSet.contains(uuid)) {
//                System.out.println("Found watch: " + Arrays.asList(uuids));
//                watchSet.remove(uuid);
//            }
            int nid = uuidIntMap.get(uuid);
            if (nid != Integer.MAX_VALUE) {
                return nid;
            }
        }
        int nid = uuidIntMap.getWithGeneration(uuids[0]);
        for (int i = 1; i < uuids.length; i++) {
            uuidIntMap.put(uuids[i], nid);
        }
        return nid;

    }

    @Override
    public int loadEconFiles(File... files) throws Exception {
        java.nio.file.Path[] paths = new java.nio.file.Path[files.length];
        for (int i = 0; i < files.length; i++) {
            paths[i] = files[i].toPath();
        }
        return loadEconFiles(paths);
    }

    @Override
    public Task<Integer> startImportLogTask(Path... paths) {
        ImportCradleLogFile importCradleLogFile = new ImportCradleLogFile(paths, this);
        ForkJoinPool.commonPool().execute(importCradleLogFile);
        return importCradleLogFile;
    }

    @Override
    public Task<Integer> startLoadTask(java.nio.file.Path... paths) {
        ImportEConceptFile loaderTask = new ImportEConceptFile(paths, this);
        ForkJoinPool.commonPool().execute(loaderTask);
        return loaderTask;
    }

    @Override
    public Task<Integer> startExportTask(Path path) {
        ExportEConceptFile exporterTask = new ExportEConceptFile(path, this);
        ForkJoinPool.commonPool().execute(exporterTask);
        return exporterTask;
    }

    @Override
    public Task<Integer> startLogicGraphExportTask(Path path) {
        ExportEConceptFile exporterTask = new ExportEConceptFile(path, this,
                (Consumer<TtkConceptChronicle>) (TtkConceptChronicle t) -> {
                    t.setRelationships(null);
                });
        ForkJoinPool.commonPool().execute(exporterTask);
        return exporterTask;
    }

    @Override
    public int loadEconFiles(java.nio.file.Path... paths) throws Exception {
        return startLoadTask(paths).get();
    }

    @Override
    public ConceptChronicleDataEager getConceptData(int i) throws IOException {
        if (i < 0) {
            i = sequenceProvider.getConceptSequence(i);
        }
        Optional<ConceptChronicleDataEager> data = conceptMap.get(i);
        if (data.isPresent()) {
            return data.get();
        }
        return new ConceptChronicleDataEager(true);
    }

    @Override
    public void addUncommitted(ConceptChronicleBI conceptChronicleBI) throws IOException {
        commitManager.addUncommitted(conceptChronicleBI);
    }

    /**
     *
     * @param conceptChronicleBI
     * @throws IOException
     */
    @Override
    public void addUncommittedNoChecks(ConceptChronicleBI conceptChronicleBI) throws IOException {
        commitManager.addUncommittedNoChecks(conceptChronicleBI);
    }

    @Override
    public Collection<? extends ConceptChronicleBI> getUncommittedConcepts() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean commit(ConceptChronicleBI conceptChronicleBI,
            ChangeSetPolicy changeSetPolicy,
            ChangeSetWriterThreading changeSetWriterThreading) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancel(ConceptChronicleBI conceptChronicleBI) throws IOException {
        commitManager.cancel(conceptChronicleBI);
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit(ConceptChronicleBI cc) throws IOException {
        int sequence = sequenceProvider.getConceptSequence(cc.getNid());
        conceptMap.put(sequence, (ConceptChronicleDataEager) ((ConceptChronicle) cc).getData());
    }

    @Override
    public void commit(ConceptVersionBI conceptVersionBI) throws IOException {
        commitManager.commit(conceptVersionBI, null);
    }

    @Override
    public void forget(ConceptChronicleBI conceptChronicleBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NativeIdSetBI isChildOfSet(int parent, ViewCoordinate viewCoordinate) {
        int[] childrenSequences = taxonomyProvider.getTaxonomyChildSequencesActive(
                sequenceProvider.getConceptSequence(parent), viewCoordinate);
        NativeIdSetBI childNidSet = new IntSet();
        IntStream.of(childrenSequences).forEach((sequence) -> childNidSet.add(sequenceProvider.getConceptNid(sequence)));
        return childNidSet;
    }

    @Override
    public NativeIdSetBI relationshipSet(int i, ViewCoordinate viewCoordinate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<DbDependency> getLatestChangeSetDependencies() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int[] getPossibleChildren(int i, ViewCoordinate viewCoordinate) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public TerminologyBuilderBI getTerminologyBuilder(EditCoordinate editCoordinate, ViewCoordinate viewCoordinate) {
        return new Builder(editCoordinate, viewCoordinate, this);
    }

    @Override
    public UUID getUuidPrimordialForNid(int nid) throws IOException {
        if (nid > 0) {
            nid = sequenceProvider.getConceptNid(nid);
        }
        ComponentChronicleBI<?> component = getComponent(nid);
        if (component != null) {
            return getComponent(nid).getPrimordialUuid();
        }
        UUID[] uuids = uuidIntMap.getKeysForValue(nid);
        log.warn("[1] No object for nid: " + nid + " Found uuids: " + Arrays.asList(uuids));
        if (uuids != null && uuids.length >= 1) {
            return uuids[0];
        }
        return null;
    }

    @Override
    public List<UUID> getUuidsForNid(int nid) throws IOException {
        ComponentChronicleBI<?> component = getComponent(nid);
        if (component != null) {
            return getComponent(nid).getUUIDs();
        }
        UUID[] uuids = uuidIntMap.getKeysForValue(nid);
        log.warn("[3] No object for nid: " + nid + " Found uuids: " + Arrays.asList(uuids));
        return Arrays.asList(uuids);
    }

    @Override
    public boolean hasPath(int i) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasUncommittedChanges() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasUuid(UUID uuid) {
        return uuidIntMap.containsKey(uuid);
    }

    @Override
    public boolean hasUuid(List<UUID> uuids) {
        return uuids.stream().anyMatch((uuid) -> (hasUuid(uuid)));
    }

    @Override
    public long getSequence() {
        return commitManager.getSequence();
    }

    @Override
    public int getConceptCount() throws IOException {
        return conceptMap.getSize();
    }

    @Override
    public ViewCoordinate getViewCoordinate(UUID uuid) throws IOException {
        return viewCoordinates.get(uuid);
    }

    @Override
    public Collection<ViewCoordinate> getViewCoordinates() throws IOException {
        return viewCoordinates.values();
    }

    @Override
    public void putViewCoordinate(ViewCoordinate viewCoordinate) throws IOException {
        viewCoordinates.put(viewCoordinate.getVcUuid(), viewCoordinate);
    }

    @Override
    public boolean isKindOf(int childNid, int parentNid, ViewCoordinate viewCoordinate) throws IOException, ContradictionException {
        return taxonomyProvider.isKindOf(childNid, parentNid, viewCoordinate);
    }

    @Override
    public boolean isChildOf(int childNid, int parentNid, ViewCoordinate viewCoordinate) throws IOException, ContradictionException {
        return taxonomyProvider.isChildOf(childNid, parentNid, viewCoordinate);
    }

    @Override
    public NativeIdSetBI isKindOfSet(int kindOfId, ViewCoordinate viewCoordinate) {
        try {
            ConcurrentBitSet bitSet = new ConcurrentBitSet(getConceptCount());
            ConceptSequenceSet sequenceSet
                    = taxonomyProvider.getKindOfSequenceSet(
                            sequenceProvider.getConceptSequence(kindOfId), viewCoordinate);
            sequenceSet.stream().forEach((int sequence) -> {
                bitSet.set(sequenceProvider.getConceptNid(sequence));
            });
            return bitSet;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void shutdown() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxReadOnlyStamp() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasConcept(int i) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLastCancel() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLastCommit() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long incrementAndGetSequence() {
        return commitManager.incrementAndGetSequence();
    }

    @Override
    public void waitTillWritesFinished() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String> getProperties() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getProperty(String s) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setProperty(String s, String s2) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancelAfterCommit(NidSetBI nidSetBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addXrefPair(int i, NidPairForRefex nidPairForRefex) throws IOException {
        // noting to do, added elsewhere...
    }

    @Override
    protected ViewCoordinate makeMetaVc() throws IOException {
        return ViewCoordinates.getMetadataViewCoordinate();
    }

    @Override
    public void forgetXrefPair(int referencedComponentNid, NidPairForRefex nidPairForRefex) throws IOException {        
        refexProvider.forgetXrefPair(referencedComponentNid, nidPairForRefex);
    }

    @Override
    public List<NidPairForRefex> getRefexPairs(int i) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int[] getDestRelOriginNids(int i, NidSetBI nidSetBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int[] getDestRelOriginNids(int i) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     *
     * @param i
     * @return
     * @throws IOException
     * @deprecated relationships are being replaced by owl 2 DL definitions, so
     * using rels directly is discouraged.
     */
    @Override
    @Deprecated
    public Collection<Relationship> getDestRels(int i) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setConceptNidForNid(int cNid, int nid) {
        if (nid < 0) {
            nid = nid - Integer.MIN_VALUE;
        }
        int conceptNidForNid = getConceptNidForNid(nid);
        if (conceptNidForNid == Integer.MAX_VALUE) {
            nidCnidMap.put(nid, cNid);
        } else if (conceptNidForNid != cNid) {
            throw new IllegalStateException("Cannot change cNid for nid: " + nid
                    + " from: " + conceptNidForNid + " to: " + nid);
        }

    }

    @Override
    public void resetConceptNidForNid(int cNid, int nid) throws IOException {
        if (nid < 0) {
            nid = nid - Integer.MIN_VALUE;
        }
        nidCnidMap.put(nid, cNid);
    }

    @Override
    public void addRelOrigin(int i, int i2) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(UUID uuid, int i) {
        uuidIntMap.put(uuid, i);
    }

    /**
     * TODO remove isIndexed/setIndexed, replaced by Lucene Indexing.
     *
     * @param i
     * @param b
     */
    @Override
    @Deprecated
    public void setIndexed(int i, boolean b) {

    }

    /**
     * TODO remove isIndexed/setIndexed, replaced by Lucene Indexing.
     *
     * @param i
     * @return
     */
    @Override
    @Deprecated
    public boolean isIndexed(int i) {
        return false;
    }

    @Override
    public ConceptChronicleDdo getFxConcept(UUID uuid, ViewCoordinate viewCoordinate) throws IOException, ContradictionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConceptChronicleDdo getFxConcept(ComponentReference componentReference, UUID uuid, VersionPolicy versionPolicy, RefexPolicy refexPolicy, RelationshipPolicy relationshipPolicy) throws IOException, ContradictionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConceptChronicleDdo getFxConcept(ComponentReference componentReference, ViewCoordinate viewCoordinate, VersionPolicy versionPolicy, RefexPolicy refexPolicy, RelationshipPolicy relationshipPolicy) throws IOException, ContradictionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConceptChronicleDdo getFxConcept(UUID uuid, UUID uuid2, VersionPolicy versionPolicy, RefexPolicy refexPolicy, RelationshipPolicy relationshipPolicy) throws IOException, ContradictionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConceptChronicleDdo getFxConcept(UUID uuid, ViewCoordinate viewCoordinate, VersionPolicy versionPolicy, RefexPolicy refexPolicy, RelationshipPolicy relationshipPolicy) throws IOException, ContradictionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addPropertyChangeListener(CONCEPT_EVENT concept_event, PropertyChangeListener propertyChangeListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addVetoablePropertyChangeListener(CONCEPT_EVENT concept_event, VetoableChangeListener vetoableChangeListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancel() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancel(ConceptVersionBI conceptVersionBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit() throws IOException {
        commitManager.commit(null);
    }

    @Override
    public boolean forget(ConceptAttributeVersionBI conceptAttributeVersionBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forget(DescriptionVersionBI descriptionVersionBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forget(RefexChronicleBI refexChronicleBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forget(RelationshipVersionBI relationshipVersionBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    private static class CradleFetcher implements ConceptFetcherBI {

        ConceptChronicleDataEager eager;

        public CradleFetcher(ConceptChronicleDataEager eager) {
            this.eager = eager;
        }

        @Override
        public ConceptChronicleBI fetch() {
            return eager.getConceptChronicle();
        }

        @Override
        public ConceptVersionBI fetch(ViewCoordinate vc) {
            return eager.getConceptChronicle().getVersion(vc);
        }

    }

    @Override
    public void iterateConceptDataInParallel(ProcessUnfetchedConceptDataBI processor) throws Exception {
        getParallelConceptDataEagerStream().forEach((concept) -> {
            try {
                processor.processUnfetchedConceptData(concept.getNid(), new CradleFetcher(concept));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    @Override
    public void iterateConceptDataInSequence(ProcessUnfetchedConceptDataBI processor) throws Exception {
        getConceptDataEagerStream().forEach((concept) -> {
            try {
                processor.processUnfetchedConceptData(concept.getNid(), new CradleFetcher(concept));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    @Override
    public void resumeChangeNotifications() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean satisfiesDependencies(Collection<DbDependency> dbDependencies) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void suspendChangeNotifications() {
        throw new UnsupportedOperationException();
    }

    @Override
    public NativeIdSetBI getAllConceptNids() throws IOException {
        ConcurrentBitSet bitSet = new ConcurrentBitSet(getConceptCount());
        sequenceProvider.getConceptSequenceStream().forEach((int sequence) -> {
            bitSet.set(sequenceProvider.getConceptNid(sequence));
        });
        return bitSet;
    }

    @Override
    public NativeIdSetBI getAllConceptNidsFromCache() throws IOException {
        ConcurrentBitSet bitSet = new ConcurrentBitSet(getConceptCount());
        sequenceProvider.getConceptSequenceStream().forEach((int sequence) -> {
            bitSet.set(sequenceProvider.getConceptNid(sequence));
        });
        return bitSet;
    }

    @Override
    public NativeIdSetBI getAllComponentNids() throws IOException {
        return nidCnidMap.getComponentNids();
    }

    @Override
    public NativeIdSetBI getConceptNidsForComponentNids(NativeIdSetBI nativeIdSet) throws IOException {
        NativeIdSetBI conceptNids = new IntSet();
        IntStream.of(nativeIdSet.getSetValues()).forEach((componentNid) -> conceptNids.add(getConceptNidForNid(componentNid)));
        return conceptNids;
    }

    @Override
    public NativeIdSetBI getComponentNidsForConceptNids(NativeIdSetBI conceptNidSet) throws IOException {
        return nidCnidMap.getKeysForValues(conceptNidSet);
    }

    @Override
    public NativeIdSetBI getOrphanNids(NativeIdSetBI nativeIdSetBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getAuthorNidForStamp(int stamp) {
        return sequenceProvider.getConceptNid(
                commitManager.getAuthorSequenceForStamp(stamp));
    }

    @Override
    public NativeIdSetBI getEmptyNidSet() throws IOException {
        return new ConcurrentBitSet(getConceptCount());
    }

    @Override
    public int getModuleNidForStamp(int stamp) {
        return sequenceProvider.getConceptNid(
                commitManager.getModuleSequenceForStamp(stamp));
    }

    @Override
    public int getPathNidForStamp(int stamp) {
        return sequenceProvider.getConceptNid(
                commitManager.getPathSequenceForStamp(stamp));
    }

    @Override
    public Status getStatusForStamp(int stamp) {
        return Status.getStatusFromState(commitManager.getStatusForStamp(stamp));
    }

    @Override
    public long getTimeForStamp(int stamp) {
        return commitManager.getTimeForStamp(stamp);
    }

    @Override
    public int getNidForUuids(Collection<UUID> uuids) throws IOException {
        return getNidForUuids(uuids.toArray(new UUID[uuids.size()]));
    }

    @Override
    public int getConceptNidForNid(int nid) {
        if (nid < 0) {
            nid = nid - Integer.MIN_VALUE;
        }
        OptionalInt returnValue = nidCnidMap.get(nid);
        if (!returnValue.isPresent()) {
            return Integer.MAX_VALUE;
        }
        return returnValue.getAsInt();
    }

    @Override
    public void writeConceptData(ConceptChronicleDataEager conceptData) {
        int sequence = sequenceProvider.getConceptSequence(conceptData.getNid());
        conceptMap.put(sequence, conceptData);
        conceptData.setPrimordial(false);
    }

    @Override
    public RefexMember<?, ?> getRefex(int refexId) {
        return refexProvider.getRefex(refexId);
    }

    @Override
    public Collection<RefexMember<?, ?>> getRefexesForAssemblage(int assemblageNid) {
        return refexProvider.getRefexesFromAssemblage(assemblageNid).collect(Collectors.toList());
    }

    @Override
    public Collection<RefexMember<?, ?>> getRefexesForComponent(int componentNid) {
        return refexProvider.getRefexesForComponent(componentNid).collect(Collectors.toList());
    }

    @Override
    public int getStamp(Status status, long time, int authorNid, int moduleNid, int pathNid) {
        return commitManager.getStamp(status.getState(),
                time,
                sequenceProvider.getConceptSequence(authorNid),
                sequenceProvider.getConceptSequence(moduleNid),
                sequenceProvider.getConceptSequence(pathNid));

    }

    @Override
    public ConcurrentSkipListSet<DestinationOriginRecord> getDestinationOriginRecordSet() {
        return ((CradleTaxonomyProvider) taxonomyProvider).getDestinationOriginRecordSet();
    }

    @Override
    public CasSequenceObjectMap<TaxonomyRecordPrimitive> getOriginDestinationTaxonomyMap() {
        return ((CradleTaxonomyProvider) taxonomyProvider).getOriginDestinationTaxonomyRecords();
    }

    @Override
    public Task<Boolean> startVerifyTask(java.nio.file.Path... paths) {
        VerifyLoadEConceptFile loaderTask = new VerifyLoadEConceptFile(paths, this);
        ForkJoinPool.commonPool().execute(loaderTask);
        return loaderTask;
    }

    @Override
    public ConceptChronicle getConcept(int cNid) {
        if (cNid >= 0) {
            return super.getConcept(sequenceProvider.getConceptNid(cNid));
        }
        return super.getConcept(cNid);
    }

    @Override
    public void index() {
        try {
            startIndexTask().get();
        } catch (InterruptedException | ExecutionException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public GenerateIndexes startIndexTask() {
        GenerateIndexes indexingTask = new GenerateIndexes(this);
        ForkJoinPool.commonPool().execute(indexingTask);
        return indexingTask;
    }

    @Override
    public Task<Integer> startLoadTask(ConceptProxy stampPath, Path... filePaths) {
        ImportEConceptFile loaderTask = new ImportEConceptFile(filePaths, this, stampPath);
        ForkJoinPool.commonPool().execute(loaderTask);
        return loaderTask;
    }

    @Override
    public Task<Boolean> startVerifyTask(ConceptProxy stampPath, Path... filePaths) {
        VerifyLoadEConceptFile loaderTask = new VerifyLoadEConceptFile(filePaths, this, stampPath);
        ForkJoinPool.commonPool().execute(loaderTask);
        return loaderTask;

    }

    @Override
    public Task<Integer> startExportTask(ConceptProxy stampPath, Path filePath) {
        ExportEConceptFile exporterTask = new ExportEConceptFile(filePath, this, ttkConceptChronicle -> {
            ttkConceptChronicle.processComponentRevisions(r -> {
                r.setPathUuid(stampPath.getUuids()[0]);
            });
        });
        ForkJoinPool.commonPool().execute(exporterTask);
        return exporterTask;
    }

    @Override
    public Task<Integer> startLogicGraphExportTask(ConceptProxy stampPath, Path filePath) {
        UUID pathUuid = stampPath.getUuids()[0];
        ExportEConceptFile exporterTask = new ExportEConceptFile(filePath, this,
                (Consumer<TtkConceptChronicle>) (TtkConceptChronicle t) -> {
                    t.setRelationships(null);
                }, (Consumer<TtkConceptChronicle>) (TtkConceptChronicle t) -> {
                    t.processComponentRevisions(r -> {
                        r.setPathUuid(pathUuid);
                    });
                });
        ForkJoinPool.commonPool().execute(exporterTask);
        return exporterTask;
    }

    @Override
    public Task<Void> addStampPathOrigin(ConceptProxy stampPath, ConceptProxy originStampPath, Instant originTime) {
        AddStampOrigin addStampOrigin = new AddStampOrigin(stampPath, originStampPath, originTime, this);
        ForkJoinPool.commonPool().execute(addStampOrigin);
        return addStampOrigin;
    }

    @Override
    public void reportStats() {
        uuidIntMap.reportStats(log);
    }

    @Override
    public Task<Integer> startLoadTask(StandardPaths stampPath, Path... filePaths) {
        switch (stampPath) {
            case DEVELOPMENT:
                return startLoadTask(IsaacMetadataAuxiliaryBinding.DEVELOPMENT, filePaths);
            case MASTER:
                return startLoadTask(IsaacMetadataAuxiliaryBinding.MASTER, filePaths);
            default:
                throw new UnsupportedOperationException("Can't handle: " + stampPath);
        }
    }

    @Override
    public Task<Boolean> startVerifyTask(StandardPaths stampPath, Path... filePaths) {
        switch (stampPath) {
            case DEVELOPMENT:
                return startVerifyTask(IsaacMetadataAuxiliaryBinding.DEVELOPMENT, filePaths);
            case MASTER:
                return startVerifyTask(IsaacMetadataAuxiliaryBinding.MASTER, filePaths);
            default:
                throw new UnsupportedOperationException("Can't handle: " + stampPath);
        }
   }

    @Override
    public Task<Integer> startExportTask(StandardPaths stampPath, Path filePath) {
        switch (stampPath) {
            case DEVELOPMENT:
                return startExportTask(IsaacMetadataAuxiliaryBinding.DEVELOPMENT, filePath);
            case MASTER:
                return startExportTask(IsaacMetadataAuxiliaryBinding.MASTER, filePath);
            default:
                throw new UnsupportedOperationException("Can't handle: " + stampPath);
        }
   }

    @Override
    public Task<Integer> startLogicGraphExportTask(StandardPaths stampPath, Path filePath) {
        switch (stampPath) {
            case DEVELOPMENT:
                return startLogicGraphExportTask(IsaacMetadataAuxiliaryBinding.DEVELOPMENT, filePath);
            case MASTER:
                return startLogicGraphExportTask(IsaacMetadataAuxiliaryBinding.MASTER, filePath);
            default:
                throw new UnsupportedOperationException("Can't handle: " + stampPath);
        }
   }

}
