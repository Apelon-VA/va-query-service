/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.commit;

import gov.vha.isaac.cradle.ConcurrentObjectIntMap;
import gov.vha.isaac.cradle.IsaacDbFolder;
import gov.vha.isaac.cradle.collections.ConcurrentSequenceSerializedObjectMap;
import gov.vha.isaac.cradle.collections.StampAliasMap;
import gov.vha.isaac.cradle.collections.StampCommentMap;
import gov.vha.isaac.cradle.collections.UuidIntMapMap;
import gov.vha.isaac.cradle.component.StampSerializer;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.ObjectChronicleTaskServer;
import gov.vha.isaac.ochre.api.SequenceProvider;
import gov.vha.isaac.ochre.api.State;
import gov.vha.isaac.ochre.api.chronicle.ChronicledConcept;
import gov.vha.isaac.ochre.api.commit.Alert;
import gov.vha.isaac.ochre.api.commit.AlertType;
import gov.vha.isaac.ochre.api.commit.ChangeChecker;
import gov.vha.isaac.ochre.api.commit.CheckPhase;
import gov.vha.isaac.ochre.api.commit.CommitManager;
import gov.vha.isaac.ochre.api.commit.CommitRecord;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import gov.vha.isaac.ochre.collections.SequenceSet;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import javafx.collections.ObservableList;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.mahout.math.map.OpenIntIntHashMap;
import org.glassfish.hk2.runlevel.RunLevel;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.ihtsdo.otf.tcc.model.cc.change.CommitSequence;
import org.ihtsdo.otf.tcc.model.cc.concept.ConceptChronicle;
import org.ihtsdo.otf.tcc.model.version.Stamp;
import org.jvnet.hk2.annotations.Service;

/**
 *
 * @author kec
 */
@Service(name = "Cradle Commit Manager")
@RunLevel(value = 1)
public class CradleCommitManager implements CommitManager {
    
    private static final Logger log = LogManager.getLogger();
    public static final String DEFAULT_CRADLE_COMMIT_MANAGER_FOLDER = "commit-manager";
    private static final String COMMIT_MANAGER_DATA_FILENAME = "commit-manager.data";
    private static final String STAMP_ALIAS_MAP_FILENAME = "stamp-alias.map";
    private static final String STAMP_COMMENT_MAP_FILENAME = "stamp-comment.map";

    private static final Map<UncommittedStamp, Integer> uncomittedStampEntries
            = new ConcurrentHashMap<>();

    private final StampAliasMap stampAliasMap = new StampAliasMap();
    private final StampCommentMap stampCommentMap = new StampCommentMap();
    private final Path dbFolderPath;
    private final Path commitManagerFolder;
    private final ConcurrentObjectIntMap<Stamp> stampMap = new ConcurrentObjectIntMap<>();
    private final ConcurrentSequenceSerializedObjectMap<Stamp> inverseStampMap;
    private final AtomicInteger nextStamp = new AtomicInteger(1);
    private final ReentrantLock stampLock = new ReentrantLock();
    private final AtomicLong databaseSequence = new AtomicLong();
    private final AtomicBoolean loadExisting = new AtomicBoolean(false);
    private final SequenceProvider sequenceProvider;
    private final ConcurrentSkipListSet<ChangeChecker> checkers = new ConcurrentSkipListSet<>();
    private final ConcurrentSkipListSet<Alert> alertCollection = new ConcurrentSkipListSet<>();

    private final ReentrantLock uncommittedSequenceLock = new ReentrantLock();
    private final ConceptSequenceSet uncommittedWithChecksSequenceSet = new ConceptSequenceSet();
    private final ConceptSequenceSet uncommittedNoChecksSequenceSet = new ConceptSequenceSet();
    
    private final WriteConceptCompletionService writeConceptCompletionService = new WriteConceptCompletionService();
    private final ExecutorService writeCompletionServicePool = Executors.newSingleThreadExecutor((Runnable r) -> {
        return new Thread(r, "writeCompletionService thread");
    });


    private long lastCommit = Long.MIN_VALUE;

    public CradleCommitManager() throws IOException {
        dbFolderPath = IsaacDbFolder.get().getDbFolderPath();
        inverseStampMap = new ConcurrentSequenceSerializedObjectMap<>(new StampSerializer(),
                dbFolderPath, null, null);
        sequenceProvider = Hk2Looker.getService(SequenceProvider.class);
        commitManagerFolder = Paths.get(dbFolderPath.toString(), DEFAULT_CRADLE_COMMIT_MANAGER_FOLDER);
    }

    @PostConstruct
    private void startMe() throws IOException {
        log.info("Starting CradleCommitManager post-construct");
        writeCompletionServicePool.submit(writeConceptCompletionService);
        if (!IsaacDbFolder.get().getPrimordial()) {
            loadExisting.set(!IsaacDbFolder.get().getPrimordial());
            log.info("Loading existing commit manager data. ");
            log.info("Loading " + COMMIT_MANAGER_DATA_FILENAME);
            try (DataInputStream in = new DataInputStream(new FileInputStream(new File(commitManagerFolder.toFile(), COMMIT_MANAGER_DATA_FILENAME)))) {
                nextStamp.set(in.readInt());
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
        }

        if (Files.exists(commitManagerFolder)) {
            log.info("Loading: " + STAMP_ALIAS_MAP_FILENAME);
            stampAliasMap.read(new File(commitManagerFolder.toFile(), STAMP_ALIAS_MAP_FILENAME));
            log.info("Loading: " + STAMP_COMMENT_MAP_FILENAME);
            stampCommentMap.read(new File(commitManagerFolder.toFile(), STAMP_COMMENT_MAP_FILENAME));
        } else {
            Files.createDirectories(commitManagerFolder);
        }

    }

    @PreDestroy
    private void stopMe() throws IOException {
        log.info("Stopping CradleCommitManager pre-destroy. ");
        log.info("nextStamp: {}", nextStamp);
        writeConceptCompletionService.cancel();
        log.info("writing: " + STAMP_ALIAS_MAP_FILENAME);
        stampAliasMap.write(new File(commitManagerFolder.toFile(), STAMP_ALIAS_MAP_FILENAME));
        log.info("writing: " + STAMP_COMMENT_MAP_FILENAME);
        stampCommentMap.write(new File(commitManagerFolder.toFile(), STAMP_COMMENT_MAP_FILENAME));
        log.info("Writing " + COMMIT_MANAGER_DATA_FILENAME);

        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(new File(commitManagerFolder.toFile(), COMMIT_MANAGER_DATA_FILENAME)))) {
            out.writeInt(nextStamp.get());
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
    public long getSequence() {
        return databaseSequence.get();
    }

    @Override
    public long incrementAndGetSequence() {
        return databaseSequence.incrementAndGet();
    }

    @Override
    public int getAuthorSequenceForStamp(int stamp) {
        Optional<Stamp> s = inverseStampMap.get(stamp);
        return sequenceProvider.getConceptSequence(
                s.get().getAuthorNid());
    }

    @Override
    public int getModuleSequenceForStamp(int stamp) {
        Optional<Stamp> s = inverseStampMap.get(stamp);
        return sequenceProvider.getConceptSequence(
                s.get().getModuleNid());
    }

    @Override
    public int getPathSequenceForStamp(int stamp) {
        Optional<Stamp> s = inverseStampMap.get(stamp);
        return sequenceProvider.getConceptSequence(
                s.get().getPathNid());
    }

    @Override
    public State getStatusForStamp(int stamp) {
        Optional<Stamp> s = inverseStampMap.get(stamp);
        return s.get().getStatus().getState();
    }

    @Override
    public long getTimeForStamp(int stamp) {
        Optional<Stamp> s = inverseStampMap.get(stamp);
        return s.get().getTime();
    }

    @Override
    public int getStamp(State status, long time, int authorSequence, int moduleSequence, int pathSequence) {
        Stamp stampKey = new Stamp(Status.getStatusFromState(status), time,
                sequenceProvider.getConceptNid(authorSequence),
                sequenceProvider.getConceptNid(moduleSequence),
                sequenceProvider.getConceptNid(pathSequence));

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

                    int stamp = nextStamp.getAndIncrement();
                    uncomittedStampEntries.put(usp, stamp);
                    inverseStampMap.put(stamp, stampKey);

                    return stamp;
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
    public void addUncommitted(ChronicledConcept cc) {
        ConceptChronicle concept = (ConceptChronicle) cc;
        handleUncommittedSequenceSet(concept, uncommittedWithChecksSequenceSet);
        writeConceptCompletionService.checkAndWrite(concept, checkers, alertCollection);
    }

    @Override
    public void addUncommittedNoChecks(ChronicledConcept cc) {
        ConceptChronicle concept = (ConceptChronicle) cc;
        handleUncommittedSequenceSet(concept, uncommittedNoChecksSequenceSet);
        writeConceptCompletionService.write(concept);
    }

    private void handleUncommittedSequenceSet(ConceptChronicle concept, ConceptSequenceSet set) {
        if (concept.isUncommitted()) {
            uncommittedSequenceLock.lock();
            try {
                set.add(sequenceProvider.getConceptSequence(concept.getNid()));
            } finally {
                uncommittedSequenceLock.unlock();
            }
        } else {
            uncommittedSequenceLock.lock();
            try {
                set.remove(sequenceProvider.getConceptSequence(concept.getNid()));
            } finally {
                uncommittedSequenceLock.unlock();
            }
        }
    }

    @Override
    public void cancel() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void cancel(ChronicledConcept cc) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public synchronized void commit(String commitComment) {

        try {
            ConceptSequenceSet conceptsToCommit = new ConceptSequenceSet();
            conceptsToCommit.or(uncommittedNoChecksSequenceSet);
            conceptsToCommit.or(uncommittedWithChecksSequenceSet);
// TODO handle notification...
//            try {
//                GlobalPropertyChange.fireVetoableChange(TerminologyStoreDI.CONCEPT_EVENT.PRE_COMMIT, null, conceptsToCommit);
//            } catch (PropertyVetoException ex) {
//                return;
//            }

            alertCollection.clear();

            lastCommit = databaseSequence.incrementAndGet();

            conceptsToCommit.stream().forEach((conceptSequence) -> {
                try {
                    ConceptChronicle c = ConceptChronicle.get(sequenceProvider.getConceptNid(conceptSequence));
                    c.modified(c.getConceptAttributes(), lastCommit);
                    if (uncommittedWithChecksSequenceSet.contains(conceptSequence)) {
                        checkers.stream().forEach((check) -> {
                            check.check(c, alertCollection, CheckPhase.ADD_UNCOMMITTED);
                        });
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            });

            for (Alert alert : alertCollection) {
                if (alert.getAlertType() == AlertType.ERROR) {
                    return; // TODO how to propigate this error?
                }
            }

            long commitTime = System.currentTimeMillis();
            SequenceSet stampSequenceSet = new SequenceSet();
            uncomittedStampEntries.entrySet().stream().forEach((entry) -> {
                int stampAsInt = entry.getValue();
                stampSequenceSet.add(stampAsInt);
                UncommittedStamp uncommittedStamp = entry.getKey();
                Stamp stamp = new Stamp(Status.getStatusFromState(entry.getKey().status),
                        commitTime,
                        sequenceProvider.getConceptNid(uncommittedStamp.authorSequence),
                        sequenceProvider.getConceptNid(uncommittedStamp.moduleSequence),
                        sequenceProvider.getConceptNid(uncommittedStamp.pathSequence));
                stampMap.put(stamp, entry.getValue());
                inverseStampMap.put(stampAsInt, stamp);
            });
            uncomittedStampEntries.clear();

            if (commitComment != null) {
                stampSequenceSet.stream().forEach((stamp)
                        -> stampCommentMap.addComment(stamp, commitComment));
            }

            if (!stampSequenceSet.isEmpty()) {
                CommitRecord commitRecord = new CommitRecord(Instant.ofEpochMilli(commitTime), 
                        stampSequenceSet.asOpenIntHashSet(),
                        new OpenIntIntHashMap(), 
                        commitComment);
            
//                ChangeSetWriterHandler handler
//                        = new ChangeSetWriterHandler(conceptsToCommit, commitTime,
//                                stampSequenceSet, ChangeSetPolicy.INCREMENTAL.convert(),
//                                ChangeSetWriterThreading.SINGLE_THREAD);
//                changeSetWriterService.execute(handler);

            }

//            notifyCommit();
            uncommittedNoChecksSequenceSet.clear();
            uncommittedWithChecksSequenceSet.clear();

//            if (indexers != null) {
//                for (IndexerBI i : indexers) {
//                    i.commitWriter();
//                }
//            }
            
//            GlobalPropertyChange.firePropertyChange(TerminologyStoreDI.CONCEPT_EVENT.POST_COMMIT, null, conceptsToCommit);

            CommitSequence.nextSequence();
        } catch (Exception e1) {
            throw new RuntimeException(e1);
        }
    }

    @Override
    public void commit(ChronicledConcept cc, String commitComment) {
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
}
