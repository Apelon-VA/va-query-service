/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.commit;

import gov.vha.isaac.cradle.ConcurrentObjectIntMap;
import gov.vha.isaac.cradle.ConcurrentSequenceIntMap;
import gov.vha.isaac.cradle.collections.ConcurrentSequenceSerializedObjectMap;
import gov.vha.isaac.cradle.collections.StampAliasMap;
import gov.vha.isaac.cradle.collections.StampCommentMap;
import gov.vha.isaac.cradle.collections.UuidIntMapMap;
import gov.vha.isaac.cradle.component.StampSerializer;
import gov.vha.isaac.ochre.api.ConfigurationService;
import gov.vha.isaac.ochre.api.Get;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.State;
import gov.vha.isaac.ochre.api.SystemStatusService;
import gov.vha.isaac.ochre.api.component.concept.ConceptChronology;
import gov.vha.isaac.ochre.api.commit.Alert;
import gov.vha.isaac.ochre.api.commit.ChangeChecker;
import gov.vha.isaac.ochre.api.commit.ChronologyChangeListener;
import gov.vha.isaac.ochre.api.commit.CommitRecord;
import gov.vha.isaac.ochre.api.commit.CommitService;
import gov.vha.isaac.ochre.api.commit.CommitStates;
import gov.vha.isaac.ochre.api.component.sememe.SememeChronology;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import gov.vha.isaac.ochre.collections.SememeSequenceSet;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.glassfish.hk2.runlevel.RunLevel;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.model.version.Stamp;
import org.jvnet.hk2.annotations.Service;

/**
 *
 * @author kec
 */
@Service(name = "Cradle Commit Provider")
@RunLevel(value = 1)
public class CommitProvider implements CommitService {

    private static final Logger log = LogManager.getLogger();

    public static final String DEFAULT_CRADLE_COMMIT_MANAGER_FOLDER = "commit-manager";
    private static final String COMMIT_MANAGER_DATA_FILENAME = "commit-manager.data";
    private static final String STAMP_ALIAS_MAP_FILENAME = "stamp-alias.map";
    private static final String STAMP_COMMENT_MAP_FILENAME = "stamp-comment.map";
    private static final int WRITE_POOL_SIZE = 40;
    private final AtomicReference<Semaphore> writePermitReference
            = new AtomicReference<>(new Semaphore(WRITE_POOL_SIZE));

    private static final Map<UncommittedStamp, Integer> uncomittedStampEntries = 
            new ConcurrentHashMap<>();

    private final StampAliasMap stampAliasMap = new StampAliasMap();
    private final StampCommentMap stampCommentMap = new StampCommentMap();
    private final Path dbFolderPath;
    private final Path commitManagerFolder;
    private final ConcurrentObjectIntMap<Stamp> stampMap = new ConcurrentObjectIntMap<>();
    private final ConcurrentSequenceSerializedObjectMap<Stamp> inverseStampMap;
    private final AtomicInteger nextStampSequence = new AtomicInteger(1);
    private final ReentrantLock stampLock = new ReentrantLock();
    private final AtomicLong databaseSequence = new AtomicLong();
    private final ConcurrentSkipListSet<ChangeChecker> checkers = new ConcurrentSkipListSet<>();
    private final ConcurrentSkipListSet<Alert> alertCollection = new ConcurrentSkipListSet<>();

    private final ReentrantLock uncommittedSequenceLock = new ReentrantLock();
    private final ConceptSequenceSet uncommittedConceptsWithChecksSequenceSet = new ConceptSequenceSet();
    private final ConceptSequenceSet uncommittedConceptsNoChecksSequenceSet = new ConceptSequenceSet();
    private final SememeSequenceSet uncommittedSememesWithChecksSequenceSet = new SememeSequenceSet();
    private final SememeSequenceSet uncommittedSememesNoChecksSequenceSet = new SememeSequenceSet();

    private final WriteConceptCompletionService writeConceptCompletionService = new WriteConceptCompletionService();
    private final WriteSememeCompletionService writeSememeCompletionService = new WriteSememeCompletionService();
    private final ExecutorService writeConceptCompletionServicePool = Executors.newSingleThreadExecutor((Runnable r) -> {
        return new Thread(r, "writeConceptCompletionService");
    });
    private final ExecutorService writeSememeCompletionServicePool = Executors.newSingleThreadExecutor((Runnable r) -> {
        return new Thread(r, "writeSememeCompletionService");
    });

    ConcurrentSkipListSet<WeakReference<ChronologyChangeListener>> changeListeners = new ConcurrentSkipListSet<>();

    private long lastCommit = Long.MIN_VALUE;
    private AtomicBoolean loadRequired = new AtomicBoolean();

    private CommitProvider() throws IOException {
        try {
            dbFolderPath = LookupService.getService(ConfigurationService.class).getChronicleFolderPath().resolve("commit-provider");
            loadRequired.set(!Files.exists(dbFolderPath));
            Files.createDirectories(dbFolderPath);
            inverseStampMap = new ConcurrentSequenceSerializedObjectMap<>(new StampSerializer(),
                    dbFolderPath, null, null);
            commitManagerFolder = dbFolderPath.resolve(DEFAULT_CRADLE_COMMIT_MANAGER_FOLDER);
             Files.createDirectories(commitManagerFolder);
       } catch (Exception e) {
            LookupService.getService(SystemStatusService.class).notifyServiceConfigurationFailure("Cradle Commit Provider", e);
            throw e;
        }
    }

    @PostConstruct
    private void startMe() throws IOException {
        try {
            log.info("Starting CradleCommitManager post-construct");
            writeConceptCompletionServicePool.submit(writeConceptCompletionService);
            writeSememeCompletionServicePool.submit(writeSememeCompletionService);
            if (!loadRequired.get()) {
                log.info("Reading existing commit manager data. ");
                log.info("Reading " + COMMIT_MANAGER_DATA_FILENAME);
                try (DataInputStream in = new DataInputStream(new FileInputStream(new File(commitManagerFolder.toFile(), COMMIT_MANAGER_DATA_FILENAME)))) {
                    nextStampSequence.set(in.readInt());
                    databaseSequence.set(in.readLong());
                    UuidIntMapMap.getNextNidProvider().set(in.readInt());
                    int stampMapSize = in.readInt();
                    for (int i = 0; i < stampMapSize; i++) {
                        int stampSequence = in.readInt();
                        Stamp stamp = new Stamp(in);
                        stampMap.put(stamp, stampSequence);
                        inverseStampMap.put(stampSequence, stamp);
                    }
                }
                log.info("Reading: " + STAMP_ALIAS_MAP_FILENAME);
                stampAliasMap.read(new File(commitManagerFolder.toFile(), STAMP_ALIAS_MAP_FILENAME));
                log.info("Reading: " + STAMP_COMMENT_MAP_FILENAME);
                stampCommentMap.read(new File(commitManagerFolder.toFile(), STAMP_COMMENT_MAP_FILENAME));
            }
        } catch (Exception e) {
            LookupService.getService(SystemStatusService.class).notifyServiceConfigurationFailure("Cradle Commit Provider", e);
            throw e;
        }
    }

    @PreDestroy
    private void stopMe() throws IOException {
        log.info("Stopping CradleCommitManager pre-destroy. ");
        log.info("nextStamp: {}", nextStampSequence);
        writeConceptCompletionService.cancel();
        writeSememeCompletionService.cancel();
        log.info("writing: " + STAMP_ALIAS_MAP_FILENAME);
        stampAliasMap.write(new File(commitManagerFolder.toFile(), STAMP_ALIAS_MAP_FILENAME));
        log.info("writing: " + STAMP_COMMENT_MAP_FILENAME);
        stampCommentMap.write(new File(commitManagerFolder.toFile(), STAMP_COMMENT_MAP_FILENAME));
        log.info("Writing " + COMMIT_MANAGER_DATA_FILENAME);

        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(new File(commitManagerFolder.toFile(), COMMIT_MANAGER_DATA_FILENAME)))) {
            out.writeInt(nextStampSequence.get());
            out.writeLong(databaseSequence.get());
            out.writeInt(UuidIntMapMap.getNextNidProvider().get());
            out.writeInt(stampMap.size());
            stampMap.forEachPair((Stamp stamp, int stampSequence) -> {
                try {
                    out.writeInt(stampSequence);
                    stamp.write(out);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            });
        }
    }

    @Override
    public void addAlias(int stamp, int stampAlias, String aliasCommitComment) {
        stampAliasMap.addAlias(stamp, stampAlias);
        if (aliasCommitComment != null) {
            stampCommentMap.addComment(stampAlias, aliasCommitComment);
        }
    }

    @Override
    public int[] getAliases(int stamp) {
        return stampAliasMap.getAliases(stamp);
    }

    @Override
    public void setComment(int stamp, String comment) {
        stampCommentMap.addComment(stamp, comment);
    }

    @Override
    public Optional<String> getComment(int stamp) {
        return stampCommentMap.getComment(stamp);
    }

    @Override
    public long getCommitManagerSequence() {
        return databaseSequence.get();
    }

    @Override
    public long incrementAndGetSequence() {
        return databaseSequence.incrementAndGet();
    }

    @Override
    public int getAuthorSequenceForStamp(int stamp) {
        Optional<Stamp> s = inverseStampMap.get(stamp);
        if (s.isPresent()) {
            return Get.identifierService().getConceptSequence(
                    s.get().getAuthorNid());
        }
        throw new NoSuchElementException("No stampSequence found: " + stamp);
    }

    public int getAuthorNidForStamp(int stamp) {
        Optional<Stamp> s = inverseStampMap.get(stamp);
        if (s.isPresent()) {
            return s.get().getAuthorNid();
        }
        throw new NoSuchElementException("No stampSequence found: " + stamp);
    }

    @Override
    public int getModuleSequenceForStamp(int stamp) {
        Optional<Stamp> s = inverseStampMap.get(stamp);
        if (s.isPresent()) {
            return Get.identifierService().getConceptSequence(
                    s.get().getModuleNid());
        }
        throw new NoSuchElementException("No stampSequence found: " + stamp);
    }
    
    private int getModuleNidForStamp(int stamp) {
        Optional<Stamp> s = inverseStampMap.get(stamp);
        if (s.isPresent()) {
            return s.get().getModuleNid();
        }
        throw new NoSuchElementException("No stampSequence found: " + stamp);
    }

    ConcurrentHashMap<Integer, Integer> stampSequencePathSequenceMap = new ConcurrentHashMap();
    @Override
    public int getPathSequenceForStamp(int stampSequence) {
        if (stampSequencePathSequenceMap.containsKey(stampSequence)) {
            return stampSequencePathSequenceMap.get(stampSequence);
        }
        Optional<Stamp> s = inverseStampMap.get(stampSequence);
        if (s.isPresent()) {
            stampSequencePathSequenceMap.put(stampSequence, Get.identifierService().getConceptSequence(
                    s.get().getPathNid()));
            return stampSequencePathSequenceMap.get(stampSequence);
        }
        throw new NoSuchElementException("No stampSequence found: " + stampSequence);
    }
    private int getPathNidForStamp(int stampSequence) {
        Optional<Stamp> s = inverseStampMap.get(stampSequence);
        if (s.isPresent()) {
            return s.get().getPathNid();
        }
        throw new NoSuchElementException("No stampSequence found: " + stampSequence);
    }

    @Override
    public State getStatusForStamp(int stampSequence) {
        Optional<Stamp> s = inverseStampMap.get(stampSequence);
        if (s.isPresent()) {
            return s.get().getStatus().getState();
        }
        throw new NoSuchElementException("No stampSequence found: " + stampSequence);
    }

    @Override
    public long getTimeForStamp(int stampSequence) {
        Optional<Stamp> s = inverseStampMap.get(stampSequence);
        if (s.isPresent()) {
            return s.get().getTime();
        }
        throw new NoSuchElementException("No stampSequence found: " + stampSequence);
    }

    @Override
    public int getRetiredStampSequence(int stampSequence) {
        return getStampSequence(State.INACTIVE, 
                getTimeForStamp(stampSequence), 
                getAuthorSequenceForStamp(stampSequence), 
                getModuleSequenceForStamp(stampSequence), 
                getPathSequenceForStamp(stampSequence));
    }
    
    @Override
    public int getActivatedStampSequence(int stampSequence) {
        return getStampSequence(State.ACTIVE, 
                getTimeForStamp(stampSequence), 
                getAuthorSequenceForStamp(stampSequence), 
                getModuleSequenceForStamp(stampSequence), 
                getPathSequenceForStamp(stampSequence));
    }

    @Override
    public int getStampSequence(State status, long time, int authorSequence, int moduleSequence, int pathSequence) {
        Stamp stampKey = new Stamp(Status.getStatusFromState(status), time,
                Get.identifierService().getConceptNid(authorSequence),
                Get.identifierService().getConceptNid(moduleSequence),
                Get.identifierService().getConceptNid(pathSequence));

        if (time == Long.MAX_VALUE) {
            UncommittedStamp usp = new UncommittedStamp(status, authorSequence,
                    moduleSequence, pathSequence);
            if (uncomittedStampEntries.containsKey(usp)) {
                return uncomittedStampEntries.get(usp);
            } else {
                stampLock.lock();

                try {
                    if (uncomittedStampEntries.containsKey(usp)) {
                        return uncomittedStampEntries.get(usp);
                    }

                    int stampSequence = nextStampSequence.getAndIncrement();
                    uncomittedStampEntries.put(usp, stampSequence);
                    inverseStampMap.put(stampSequence, stampKey);

                    return stampSequence;
                } finally {
                    stampLock.unlock();
                }
            }
        }

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
                    stampValue = OptionalInt.of(nextStampSequence.getAndIncrement());
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
    public Task<Void> cancel() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Task<Void> cancel(ConceptChronology cc) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Task<Void> cancel(SememeChronology sememeChronicle) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * Perform a global commit. The caller may chose to block on the returned
     * task if synchronous operation is desired. 
     * @param commitComment
     * @return a task that is already submitted to an executor. 
     */
    @Override
    public synchronized Task<Optional<CommitRecord>> commit(String commitComment) {
        Semaphore pendingWrites = writePermitReference.getAndSet(new Semaphore(WRITE_POOL_SIZE));
        pendingWrites.acquireUninterruptibly(WRITE_POOL_SIZE);
        alertCollection.clear();
        lastCommit = databaseSequence.incrementAndGet();
        
        Map<UncommittedStamp, Integer> pendingStampsForCommit = new HashMap<>(uncomittedStampEntries);
        uncomittedStampEntries.clear();
        
        CommitTask task = CommitTask.get(commitComment,
            uncommittedConceptsWithChecksSequenceSet,
            uncommittedConceptsNoChecksSequenceSet,
            uncommittedSememesWithChecksSequenceSet,
            uncommittedSememesNoChecksSequenceSet,
            lastCommit,
            checkers,
            alertCollection,
            pendingStampsForCommit,
            this);
        return task;
    }
    
    protected void revertCommit(ConceptSequenceSet conceptsToCommit,
            ConceptSequenceSet conceptsToCheck,
            SememeSequenceSet sememesToCommit, 
            SememeSequenceSet sememesToCheck,
            Map<UncommittedStamp, Integer> pendingStampsForCommit) {
        
        uncomittedStampEntries.putAll(pendingStampsForCommit);
        uncommittedSequenceLock.lock();
        try {
            uncommittedConceptsWithChecksSequenceSet.or(conceptsToCheck);
            uncommittedConceptsNoChecksSequenceSet.or(conceptsToCommit);
            uncommittedConceptsNoChecksSequenceSet.andNot(conceptsToCheck);
            
            uncommittedSememesWithChecksSequenceSet.or(sememesToCheck);
            uncommittedSememesNoChecksSequenceSet.or(sememesToCommit);
            uncommittedSememesNoChecksSequenceSet.andNot(sememesToCheck);
        }finally {
            uncommittedSequenceLock.unlock();
        }
        
    }

    @Override
    public Task<Optional<CommitRecord>> commit(ConceptChronology cc, String commitComment) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Task<Optional<CommitRecord>> commit(SememeChronology cc, String commitComment) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ObservableList<Integer> getUncommittedConceptNids() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ObservableList<Alert> getAlertList() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void addChangeChecker(ChangeChecker checker) {
        checkers.add(checker);
    }

    @Override
    public void removeChangeChecker(ChangeChecker checker) {
        checkers.remove(checker);
    }

    @Override
    public boolean isNotCanceled(int stamp) {
        if (stamp < 0) {
            return false;
        }
        return getTimeForStamp(stamp) != Long.MIN_VALUE;
    } 

    @Override
    public String describeStampSequence(int stampSequence) {
        StringBuilder sb = new StringBuilder();
        sb.append("⦙");
        sb.append(stampSequence);
        sb.append("::");
        sb.append(getStatusForStamp(stampSequence));
        sb.append(" ");
        long time = getTimeForStamp(stampSequence);
        if (time == Long.MAX_VALUE) {
            sb.append("UNCOMMITTED:");
        } else if (time == Long.MIN_VALUE) {
            sb.append("CANCELED:");
        } else {
            sb.append(Instant.ofEpochMilli(time));
        }
        sb.append(" a:");
        sb.append(Get.conceptDescriptionText(getAuthorSequenceForStamp(stampSequence)));
        sb.append(" <");
        sb.append(getAuthorSequenceForStamp(stampSequence));
        sb.append(">");
        sb.append(" m:");
        sb.append(Get.conceptDescriptionText(getModuleSequenceForStamp(stampSequence)));
        sb.append(" <");
        sb.append(getModuleSequenceForStamp(stampSequence));
        sb.append(">");
        sb.append(" p: ");
        sb.append(Get.conceptDescriptionText(getPathSequenceForStamp(stampSequence)));
        sb.append(" <");
        sb.append(getPathSequenceForStamp(stampSequence));
        sb.append(">⦙");
        return sb.toString();
    }

    @Override
    public IntStream getStampSequences() {
        return IntStream.rangeClosed(1, nextStampSequence.get()).
                filter((stampSequence) -> inverseStampMap.containsKey(stampSequence));
    }

    @Override
    public Task<Void> addUncommitted(SememeChronology sc) {
        handleUncommittedSequenceSet(sc, uncommittedSememesWithChecksSequenceSet);
        return writeSememeCompletionService.checkAndWrite(sc, checkers, alertCollection,
                writePermitReference.get(), changeListeners);
    }

    @Override
    public Task<Void> addUncommittedNoChecks(SememeChronology sc) {
        handleUncommittedSequenceSet(sc, uncommittedSememesNoChecksSequenceSet);
        return writeSememeCompletionService.write(sc,
                writePermitReference.get(), changeListeners);
    }

    @Override
    public Task<Void> addUncommitted(ConceptChronology cc) {
        handleUncommittedSequenceSet(cc, uncommittedConceptsWithChecksSequenceSet);
        return writeConceptCompletionService.checkAndWrite(cc, checkers, alertCollection,
                writePermitReference.get(), changeListeners);
    }

    @Override
    public Task<Void> addUncommittedNoChecks(ConceptChronology cc) {
        handleUncommittedSequenceSet(cc, uncommittedConceptsNoChecksSequenceSet);
        return writeConceptCompletionService.write(cc,
                writePermitReference.get(), changeListeners);
    }

    private void handleUncommittedSequenceSet(SememeChronology sememeChronicle, SememeSequenceSet set) {
        if (sememeChronicle.getCommitState() == CommitStates.UNCOMMITTED) {
            uncommittedSequenceLock.lock();
            try {
                set.add(Get.identifierService().getSememeSequence(sememeChronicle.getNid()));
            } finally {
                uncommittedSequenceLock.unlock();
            }
        } else {
            uncommittedSequenceLock.lock();
            try {
                set.remove(Get.identifierService().getSememeSequence(sememeChronicle.getNid()));
            } finally {
                uncommittedSequenceLock.unlock();
            }
        }
    }

    private void handleUncommittedSequenceSet(ConceptChronology concept, ConceptSequenceSet set) {
        if (concept.isUncommitted()) {
            uncommittedSequenceLock.lock();
            try {
                set.add(Get.identifierService().getConceptSequence(concept.getNid()));
            } finally {
                uncommittedSequenceLock.unlock();
            }
        } else {
            uncommittedSequenceLock.lock();
            try {
                set.remove(Get.identifierService().getConceptSequence(concept.getNid()));
            } finally {
                uncommittedSequenceLock.unlock();
            }
        }
    }

    @Override
    public boolean isUncommitted(int stampSequence) {
        return getTimeForStamp(stampSequence) == Long.MAX_VALUE;
    }
    
    protected void addComment(int stamp, String commitComment) {
        stampCommentMap.addComment(stamp, commitComment);
    }
    
    protected void addStamp(Stamp stamp, int stampSequence) {
        stampMap.put(stamp, stampSequence);
        inverseStampMap.put(stampSequence, stamp);
    }

    @Override
    public void addChangeListener(ChronologyChangeListener changeListener) {
        changeListeners.add(new ChangeListenerReference(changeListener));
    }

    @Override
    public void removeChangeListener(ChronologyChangeListener changeListener) {
        changeListeners.remove(new ChangeListenerReference(changeListener));
    }

    private static class ChangeListenerReference extends WeakReference<ChronologyChangeListener> implements Comparable<ChangeListenerReference> {

        UUID listenerUuid;
        public ChangeListenerReference(ChronologyChangeListener referent) {
            super(referent);
            this.listenerUuid = referent.getListenerUuid();
        }

        public ChangeListenerReference(ChronologyChangeListener referent, ReferenceQueue<? super ChronologyChangeListener> q) {
            super(referent, q);
            this.listenerUuid = referent.getListenerUuid();
        }

        @Override
        public int compareTo(ChangeListenerReference o) {
            return this.listenerUuid.compareTo(o.listenerUuid);
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 67 * hash + Objects.hashCode(this.listenerUuid);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final ChangeListenerReference other = (ChangeListenerReference) obj;
            return Objects.equals(this.listenerUuid, other.listenerUuid);
        }
    
    }

    @Override
    public boolean stampSequencesEqualExceptAuthorAndTime(int stampSequence1, int stampSequence2) {
        if (getModuleNidForStamp(stampSequence1) != getModuleNidForStamp(stampSequence2)) {
            return false;
        }
        if (getPathNidForStamp(stampSequence1) != getPathNidForStamp(stampSequence2)) {
            return false;
        }
        return getStatusForStamp(stampSequence1) == getStatusForStamp(stampSequence2);
    }
    
}
