package gov.vha.isaac.cradle;

import gov.vha.isaac.cradle.tasks.*;
import gov.vha.isaac.cradle.taxonomy.PrimitiveTaxonomyRecordSerializer;
import gov.vha.isaac.cradle.taxonomy.PrimitiveTaxonomyRecord;
import gov.vha.isaac.cradle.collections.CasSequenceObjectMap;
import gov.vha.isaac.cradle.collections.ConcurrentSequenceSerializedObjectMap;
import gov.vha.isaac.cradle.collections.SequenceMap;
import gov.vha.isaac.cradle.collections.UuidIntMapMap;
import gov.vha.isaac.cradle.component.ConceptChronicleDataEager;
import gov.vha.isaac.cradle.component.ConceptChronicleDataEagerSerializer;
import gov.vha.isaac.cradle.component.SememeSerializer;
import gov.vha.isaac.cradle.component.StampSerializer;
import gov.vha.isaac.cradle.taxonomy.StampRecordsUnpacked;
import gov.vha.isaac.cradle.taxonomy.TaxonomyFlags;
import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordUnpacked;
import gov.vha.isaac.cradle.version.StampSequenceComputer;
import gov.vha.isaac.cradle.version.ViewPoint;
import gov.vha.isaac.ochre.api.ConceptProxy;
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
import org.ihtsdo.otf.tcc.api.coordinate.Precedence;
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
import org.ihtsdo.otf.tcc.dto.component.TtkRevision;
import org.ihtsdo.otf.tcc.dto.component.TtkRevisionProcessorBI;
import org.ihtsdo.otf.tcc.model.cc.NidPairForRefex;
import org.ihtsdo.otf.tcc.model.cc.concept.ConceptChronicle;
import org.ihtsdo.otf.tcc.model.cc.refex.RefexMember;
import org.ihtsdo.otf.tcc.model.cc.relationship.Relationship;
import org.ihtsdo.otf.tcc.model.cc.termstore.Termstore;
import org.ihtsdo.otf.tcc.model.version.Stamp;
import org.jvnet.hk2.annotations.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.beans.PropertyChangeListener;
import java.beans.VetoableChangeListener;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.text.ParseException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.mahout.math.set.OpenIntHashSet;
import gov.vha.isaac.lookup.constants.Constants;
import gov.vha.isaac.ochre.api.ObjectChronicleTaskServer;
import gov.vha.isaac.ochre.api.SequenceProvider;
import java.nio.file.Path;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import org.ihtsdo.otf.tcc.api.chronicle.ComponentChronicleBI;
import org.ihtsdo.otf.tcc.api.nid.ConcurrentBitSet;
import org.ihtsdo.otf.tcc.dto.TtkConceptChronicle;

/**
 * Created by kec on 12/18/14.
 */
@Service(name = "ChRonicled Assertion Database of Logical Expressions")
@RunLevel(value = 1)
public class Cradle
        extends Termstore
        implements ObjectChronicleTaskServer, CradleExtensions, SequenceProvider {

    public static final String DEFAULT_ISAACDB_FOLDER = "isaacDb";
    private static final Logger log = LogManager.getLogger();

    final UuidIntMapMap uuidIntMap = new UuidIntMapMap();

    final ConcurrentSequenceSerializedObjectMap<ConceptChronicleDataEager> conceptMap = new ConcurrentSequenceSerializedObjectMap(new ConceptChronicleDataEagerSerializer());
    final ConcurrentSequenceSerializedObjectMap<RefexMember<?, ?>> sememeMap = new ConcurrentSequenceSerializedObjectMap(new SememeSerializer());

    final ConcurrentObjectIntMap<Stamp> stampMap = new ConcurrentObjectIntMap<>();
    final ConcurrentSequenceSerializedObjectMap<Stamp> inverseStampMap = new ConcurrentSequenceSerializedObjectMap<>(new StampSerializer());
    final AtomicInteger nextStamp = new AtomicInteger(1);
    final AtomicLong databaseSequence = new AtomicLong();
    final SequenceMap conceptSequence = new SequenceMap();
    final SequenceMap sememeSequence = new SequenceMap();
    final ConcurrentSequenceIntMap nidCnidMap = new ConcurrentSequenceIntMap();
    final CasSequenceObjectMap<PrimitiveTaxonomyRecord> taxonomyRecords = new CasSequenceObjectMap(new PrimitiveTaxonomyRecordSerializer());

    final ConcurrentSkipListSet<SememeKey> assemblageNidReferencedNidSememeSequenceMap = new ConcurrentSkipListSet<>();
    final ConcurrentSkipListSet<SememeKey> referencedNidAssemblageNidSememeSequenceMap = new ConcurrentSkipListSet<>();

    transient HashMap<UUID, ViewCoordinate> viewCoordinates = new HashMap<>();

    java.nio.file.Path dbFolderPath;

    AtomicBoolean loadExisting = new AtomicBoolean(false);

    Cradle() throws IOException, NumberFormatException, ParseException {

    }

    @PostConstruct
    private void startMe() throws IOException {
        log.info("Starting Cradle post-construct");

        String issacDbRootFolder = System.getProperty(Constants.CHRONICLE_COLLECTIONS_ROOT_LOCATION_PROPERTY);
        if (issacDbRootFolder == null || issacDbRootFolder.isEmpty()) {
                throw new IllegalStateException(Constants.CHRONICLE_COLLECTIONS_ROOT_LOCATION_PROPERTY + 
                        " has not been set.");
        }
        

        dbFolderPath = java.nio.file.Paths.get(issacDbRootFolder, DEFAULT_ISAACDB_FOLDER);
        if (Files.exists(dbFolderPath)) {
            loadExisting.set(true);
        } else {
            Files.createDirectories(dbFolderPath);
        }
    }

    @Override
    public void loadExistingDatabase() throws IOException {
        if (loadExisting.compareAndSet(true, false)) {
            log.info("Loading existing database. ");
        log.info("Loading isaac.data.");
            try (DataInputStream in = new DataInputStream(new FileInputStream(new File(dbFolderPath.toFile(), "isaac.data")))) {
                nextStamp.set(in.readInt());
                databaseSequence.set(in.readLong());
                int[] nextNids = new int[in.readInt()];
                for (int i = 0; i < nextNids.length; i++) {
                    nextNids[i] = in.readInt();
                }
                int stampMapSize = in.readInt();
                for (int i = 0; i < stampMapSize; i++) {
                    int stampSequence = in.readInt();
                    Stamp stamp = new Stamp(in);
                    stampMap.put(stamp, stampSequence);
                    inverseStampMap.put(stampSequence, stamp);
                }
            }

        log.info("Loading sequence-cnid-map.");
            nidCnidMap.read(new File(dbFolderPath.toFile(), "sequence-cnid-map"));
        log.info("Loading concept-sequence.map.");
           conceptSequence.read(new File(dbFolderPath.toFile(), "concept-sequence.map"));
        log.info("Loading sememe-sequence.map.");
            sememeSequence.read(new File(dbFolderPath.toFile(), "sememe-sequence.map"));

        log.info("Loading concept-map.");
            conceptMap.read(dbFolderPath, "concept-map/", ".concepts.map");
        log.info("Loading uuid-nid-map.");
            uuidIntMap.read(new File(dbFolderPath.toFile(), "uuid-nid-map"));

            /*
            conceptMap.getParallelStream().forEach((ConceptChronicleDataEager conceptData) -> {
                //setConceptNidForNid(conceptData.getNid(), conceptData.getNid());
                conceptData.getConceptComponents().forEach((ConceptComponent<?, ?> component) -> {
                    //setConceptNidForNid(conceptData.getNid(), component.getNid());
//                    component.getUUIDs().stream().forEach((uuid) -> {
//                        uuidIntMap.put(uuid, component.getNid());
//                    });
                });
            });
            */

        //log.info("Loading taxonomy.");
        //    IsaacStartupAccumulator accumulator = conceptMap.getParallelStream().collect(new IsaacStartupCollector(this));

        log.info("Reading taxonomy.");
        taxonomyRecords.read(dbFolderPath, "taxonomy/", ".taxonomy.map");
            
        log.info("Loading sememeMap.");
            sememeMap.read(dbFolderPath, "sememe-map/", ".sememe.map");
            
            
        log.info("Loading SememeKeys.");
        
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(new File(dbFolderPath.toFile(), "sememe.keys"))))) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                int key1 = in.readInt();
                int key2 = in.readInt();
                int sequence = in.readInt();
                assemblageNidReferencedNidSememeSequenceMap.add(new SememeKey(key1, key2, sequence));
                referencedNidAssemblageNidSememeSequenceMap.add(new SememeKey(key2, key1, sequence));
            }
        }
        log.info("Finished load.");
 
/*
            sememeMap.getParallelStream().forEach((RefexMember<?, ?> sememe) -> {
                assemblageNidReferencedNidSememeSequenceMap.add(new SememeKey(sememe.assemblageNid,
                        sememe.referencedComponentNid, sememeSequence.getSequence(sememe.nid).getAsInt()));
                referencedNidAssemblageNidSememeSequenceMap.add(new SememeKey(sememe.referencedComponentNid,
                        sememe.assemblageNid, sememeSequence.getSequence(sememe.nid).getAsInt()));
                //setConceptNidForNid(sememe.assemblageNid, sememe.nid);
//                sememe.getUUIDs().stream().forEach((uuid) -> {
//                    uuidIntMap.put(uuid, sememe.getNid());
//                });
            });
*/
        }

    }

    @PreDestroy
    private void stopMe() throws IOException {
        log.info("Stopping Cradle pre-destroy. ");

        log.info("conceptSequence: {}", conceptSequence.getNextSequence());
        log.info("nextStamp: {}", nextStamp);
        log.info("sememeMap size: {}", sememeMap.getSize());

        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(new File(dbFolderPath.toFile(), "isaac.data")))) {
            out.writeInt(nextStamp.get());
            out.writeLong(databaseSequence.get());
            int[] nextNids = uuidIntMap.getNextIdArray();
            out.writeInt(nextNids.length);
            for (int nextNid : nextNids) {
                out.writeInt(nextNid);
            }
            out.writeInt(stampMap.size());
            stampMap.backingMap.forEachPair((Stamp stamp, int stampSequence) -> {
                try {
                    out.writeInt(stampSequence);
                    stamp.write(out);
                    return true;
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            });
        }

        log.info("writing concept-sequence.map.");
        conceptSequence.write(new File(dbFolderPath.toFile(), "concept-sequence.map"));
        log.info("writing sememe-sequence.map.");
        sememeSequence.write(new File(dbFolderPath.toFile(), "sememe-sequence.map"));

        log.info("writing concept-map.");
        conceptMap.write(dbFolderPath, "concept-map/", ".concepts.map");
        log.info("writing sememe-map.");
        sememeMap.write(dbFolderPath, "sememe-map/", ".sememe.map");
        
        log.info("writing SememeKeys.");
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(dbFolderPath.toFile(), "sememe.keys"))))) {
            out.writeInt(assemblageNidReferencedNidSememeSequenceMap.size());
            for (SememeKey key: assemblageNidReferencedNidSememeSequenceMap) {
                out.writeInt(key.key1);
                out.writeInt(key.key2);
                out.writeInt(key.sememeSequence);
            }
        }
        log.info("writing uuid-nid-map.");
        uuidIntMap.write(new File(dbFolderPath.toFile(), "uuid-nid-map"));

        log.info("writing sequence-cnid-map.");
        nidCnidMap.write(new File(dbFolderPath.toFile(), "sequence-cnid-map"));
        
        log.info("Writing taxonomy.");
        taxonomyRecords.write(dbFolderPath, "taxonomy/", ".taxonomy.map");

        log.info("Finished pre-destroy.");
    }

    @Override
    public void writeSememe(RefexMember<?, ?> sememe) {
        int sequence = sememeSequence.addNidIfMissing(sememe.getNid());
        if (!sememeMap.containsKey(sequence)) {
            assemblageNidReferencedNidSememeSequenceMap.add(new SememeKey(sememe.assemblageNid,
                    sememe.referencedComponentNid, sequence));
            referencedNidAssemblageNidSememeSequenceMap.add(new SememeKey(sememe.referencedComponentNid,
                    sememe.assemblageNid,
                    sequence));
        }
        sememeMap.put(sequence, sememe);
    }

    @Override
    public IntStream getConceptSequenceStream() {
        return conceptSequence.getConceptSequenceStream();
    }

    @Override
    public IntStream getParallelConceptSequenceStream() {
        return conceptSequence.getConceptSequenceStream().parallel();
    }

    @Override
    public int getConceptSequence(int nid) {
        return conceptSequence.addNidIfMissing(nid);
    }

    public OptionalInt getConceptNidForConceptSequence(int sequence) {
        return conceptSequence.getNid(sequence);
    }

    @Override
    public int getConceptNid(int conceptSequence) {
        return this.conceptSequence.getNidFast(conceptSequence);
    }

    @Override
    public int getSememeSequence(int nid) {
        return sememeSequence.addNidIfMissing(nid);
    }

    @Override
    public int getSememeNid(int sememeSequence) {
        return this.sememeSequence.getNidFast(sememeSequence);
    }
    
    

    @Override
    public Stream<ConceptChronicleDataEager> getConceptDataEagerStream() {
        return conceptMap.getStream();
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
    public int getNidForUuids(UUID... uuids) throws IOException {

        for (UUID uuid : uuids) {
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
        LoadEConceptFile loaderTask = new LoadEConceptFile(paths, this);
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
            i = conceptSequence.addNidIfMissing(i);
        }
        Optional<ConceptChronicleDataEager> data = conceptMap.get(i);
        if (data.isPresent()) {
            return data.get();
        }
        return new ConceptChronicleDataEager(true);
    }

    @Override
    public void addUncommitted(ConceptChronicleBI conceptChronicleBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * TODO checks should be before the termstore, not part of the termstore...
     *
     * @param conceptChronicleBI
     * @throws IOException
     */
    @Override
    public void addUncommittedNoChecks(ConceptChronicleBI conceptChronicleBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<? extends ConceptChronicleBI> getUncommittedConcepts() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean commit(ConceptChronicleBI conceptChronicleBI, ChangeSetPolicy changeSetPolicy, ChangeSetWriterThreading changeSetWriterThreading) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancel(ConceptChronicleBI conceptChronicleBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit(ConceptChronicleBI cc) throws IOException {
        int sequence = conceptSequence.addNidIfMissing(cc.getNid());
        conceptMap.put(sequence, (ConceptChronicleDataEager) ((ConceptChronicle) cc).getData());
    }

    @Override
    public void commit(ConceptVersionBI conceptVersionBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forget(ConceptChronicleBI conceptChronicleBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NativeIdSetBI isChildOfSet(int i, ViewCoordinate viewCoordinate) {
        throw new UnsupportedOperationException();
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
    public UUID getUuidPrimordialForNid(int i) throws IOException {
        if (i > 0) {
            i = getConceptNid(i);
        }
        ComponentChronicleBI<?> component = getComponent(i);
        if (component != null) {
            return getComponent(i).getPrimordialUuid();
        }
        UUID[] uuids = uuidIntMap.getKeysForValue(i);
        log.warn("[1] No object for nid: "+ i +" Found uuids: " + Arrays.asList(uuids));
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
        log.warn("[3] No object for nid: "+nid+" Found uuids: " + Arrays.asList(uuids));
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
        return databaseSequence.get();
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

        ViewPoint viewPoint = new ViewPoint(viewCoordinate.getViewPosition(),
                new OpenIntHashSet(), Precedence.PATH);

        EnumSet<TaxonomyFlags> flags = TaxonomyFlags.getFlagsFromRelationshipAssertionType(viewCoordinate);

        int childSequence = getConceptSequence(childNid);
        int parentSequence = getConceptSequence(parentNid);

        return recursiveFindAncestor(childSequence, parentSequence, flags, viewPoint);

    }

    private boolean recursiveFindAncestor(int childSequence, int parentSequence, EnumSet<TaxonomyFlags> flags, ViewPoint viewPoint) {
        // currently unpacking from array to object.
        // TODO operate directly on array if unpacking is a performance bottleneck.

        Optional<PrimitiveTaxonomyRecord> record = taxonomyRecords.get(childSequence);
        if (record.isPresent()) {
            TaxonomyRecordUnpacked childTaxonomyRecords = new TaxonomyRecordUnpacked(record.get().getArray());
            int[] activeConceptSequences
                    = childTaxonomyRecords.getActiveConceptSequences(flags, viewPoint).toArray();
            if (Arrays.stream(activeConceptSequences).anyMatch((int activeParentSequence) -> activeParentSequence == parentSequence)) {
                return true;
            }
            return Arrays.stream(activeConceptSequences).anyMatch(
                    (int intermediateChild) -> recursiveFindAncestor(intermediateChild, parentSequence, flags, viewPoint));
        }
        return false;
    }

    @Override
    public boolean isChildOf(int childNid, int parentNid, ViewCoordinate viewCoordinate) throws IOException, ContradictionException {
        ViewPoint viewPoint = new ViewPoint(viewCoordinate.getViewPosition(),
                new OpenIntHashSet(), Precedence.PATH);
        StampSequenceComputer computer = StampSequenceComputer.getComputer(viewPoint);
        EnumSet<TaxonomyFlags> flags = TaxonomyFlags.getFlagsFromRelationshipAssertionType(viewCoordinate);

        int childSequence = getConceptSequence(childNid);
        int parentSequence = getConceptSequence(parentNid);

        Optional<PrimitiveTaxonomyRecord> record = taxonomyRecords.get(childSequence);
        if (record.isPresent()) {
            TaxonomyRecordUnpacked childTaxonomyRecords = new TaxonomyRecordUnpacked(record.get().getArray());
            Optional<StampRecordsUnpacked> parentStampRecordsOptional = childTaxonomyRecords.getConceptSequenceStampRecords(parentSequence);
            if (parentStampRecordsOptional.isPresent()) {
                StampRecordsUnpacked parentStampRecords = parentStampRecordsOptional.get();
                IntStream.Builder parentStampsIntStream = IntStream.builder();
                parentStampRecords.forEachPair((int stamp, EnumSet<TaxonomyFlags> flagsForStamp) -> {
                    if (flagsForStamp.containsAll(flags)) {
                        parentStampsIntStream.add(stamp);
                    }
                    return true;
                });
                if (computer.isLatestActive(parentStampsIntStream.build())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public NativeIdSetBI isKindOfSet(int i, ViewCoordinate viewCoordinate) {
        throw new UnsupportedOperationException();
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
        return databaseSequence.incrementAndGet();
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
    public void forgetXrefPair(int referencedComponentNid, NidPairForRefex nidPairForRefex) throws IOException {
        OptionalInt sequence = sememeSequence.getSequence(nidPairForRefex.getMemberNid());
        if (sequence.isPresent()) {
            assemblageNidReferencedNidSememeSequenceMap.remove(new SememeKey(nidPairForRefex.getRefexNid(), referencedComponentNid, sequence.getAsInt()));
            referencedNidAssemblageNidSememeSequenceMap.remove(new SememeKey(referencedComponentNid, nidPairForRefex.getRefexNid(), sequence.getAsInt()));
        }
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

    @Override
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
        throw new UnsupportedOperationException();
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

    @Override
    public void iterateConceptDataInParallel(ProcessUnfetchedConceptDataBI processUnfetchedConceptDataBI) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void iterateConceptDataInSequence(ProcessUnfetchedConceptDataBI processUnfetchedConceptDataBI) throws Exception {
        throw new UnsupportedOperationException();
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
        conceptSequence.getConceptNidStream().forEach((int nid) -> {
            bitSet.set(nid);
        });
        return bitSet;
    }

    @Override
    public NativeIdSetBI getAllConceptNidsFromCache() throws IOException {
        ConcurrentBitSet bitSet = new ConcurrentBitSet(getConceptCount());
        conceptSequence.getConceptNidStream().forEach((int nid) -> {
            bitSet.set(nid);
        });
        return bitSet;
    }

    @Override
    public NativeIdSetBI getAllComponentNids() throws IOException {
        return nidCnidMap.getComponentNids();
    }

    @Override
    public NativeIdSetBI getConceptNidsForComponentNids(NativeIdSetBI nativeIdSetBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NativeIdSetBI getComponentNidsForConceptNids(NativeIdSetBI nativeIdSetBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NativeIdSetBI getOrphanNids(NativeIdSetBI nativeIdSetBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getAuthorNidForStamp(int i) {
        Optional<Stamp> s = inverseStampMap.get(i);
        return s.get().getAuthorNid();
    }

    @Override
    public NativeIdSetBI getEmptyNidSet() throws IOException {
        return new ConcurrentBitSet(getConceptCount());
    }

    @Override
    public int getModuleNidForStamp(int i) {
        Optional<Stamp> s = inverseStampMap.get(i);
        return s.get().getModuleNid();
    }

    @Override
    public int getPathNidForStamp(int i) {
        Optional<Stamp> s = inverseStampMap.get(i);
        return s.get().getPathNid();
    }

    @Override
    public Status getStatusForStamp(int i) {
        Optional<Stamp> s = inverseStampMap.get(i);
        return s.get().getStatus();
    }

    @Override
    public long getTimeForStamp(int i) {
        Optional<Stamp> s = inverseStampMap.get(i);
        return s.get().getTime();
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
        int sequence = conceptSequence.addNidIfMissing(conceptData.getNid());
        conceptMap.put(sequence, conceptData);
        conceptData.setPrimordial(false);
    }

    @Override
    public RefexMember<?, ?> getSememe(int sememeNid) {
        if (sememeNid < 0) {
            sememeNid = sememeSequence.addNidIfMissing(sememeNid);
        }
        Optional<RefexMember<?, ?>> optionalResult = sememeMap.get(sememeNid);
        if (optionalResult.isPresent()) {
            return optionalResult.get();
        }
        return null;
    }

    @Override
    public Collection<RefexMember<?, ?>> getSememesForAssemblage(int assemblageNid) {

        SememeKey rangeStart = new SememeKey(assemblageNid, Integer.MIN_VALUE, Integer.MIN_VALUE); // yes
        SememeKey rangeEnd = new SememeKey(assemblageNid, Integer.MAX_VALUE, Integer.MAX_VALUE); // no
        NavigableSet<SememeKey> assemblageSememeKeys
                = assemblageNidReferencedNidSememeSequenceMap.subSet(rangeStart, true,
                        rangeEnd, true
                );
        return new SememeCollection(assemblageSememeKeys, sememeMap);
    }

    @Override
    public Collection<RefexMember<?, ?>> getSememesForComponent(int componentNid) {

        NavigableSet<SememeKey> assemblageSememeKeys
                = referencedNidAssemblageNidSememeSequenceMap.subSet(
                        new SememeKey(componentNid, Integer.MIN_VALUE, Integer.MIN_VALUE), true,
                        new SememeKey(componentNid, Integer.MAX_VALUE, Integer.MAX_VALUE), true
                );

        return new SememeCollection(assemblageSememeKeys, sememeMap);
    }

    ReentrantLock stampLock = new ReentrantLock();

    @Override
    public int getStamp(Status status, long time, int authorNid, int moduleNid, int pathNid) {
        if (time == Long.MAX_VALUE) {
            throw new UnsupportedOperationException("Can't handle commit yet...");
        }
        Stamp stampKey = new Stamp(status, time, authorNid, moduleNid, pathNid);
        OptionalInt stampValue = stampMap.get(stampKey);
        if (!stampValue.isPresent()) {
            // maybe have a few available in an atomic queue, and put back
            // if not used? Maybe in a thread-local?
            // Have different sequences, and have the increments be equal to the
            // number of sequences?
            stampLock.lock();
            try {
                stampValue = stampMap.get(stampKey);
                if (!stampValue.isPresent()) {
                    stampValue = OptionalInt.of(nextStamp.getAndIncrement());
                    inverseStampMap.put(stampValue.getAsInt(), stampKey);
                    stampMap.put(stampKey, stampValue.getAsInt());
                }
            } finally {
                stampLock.unlock();
            }
        }
        return stampValue.getAsInt();
    }

    @Override
    public CasSequenceObjectMap<PrimitiveTaxonomyRecord> getTaxonomyMap() {
        return taxonomyRecords;
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
            OptionalInt nidForSequence = getConceptNidForConceptSequence(cNid);
            if (nidForSequence.isPresent()) {
                return super.getConcept(nidForSequence.getAsInt());
            } else {
                throw new IllegalStateException("No nid for sequence: " + cNid);
            }
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
        LoadEConceptFile loaderTask = new LoadEConceptFile(filePaths, this, stampPath);
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

}
