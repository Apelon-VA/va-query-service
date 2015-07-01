/*
 * Copyright 2015 kec.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gov.vha.isaac.cradle.commit;

import gov.vha.isaac.ochre.api.Get;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.commit.Alert;
import gov.vha.isaac.ochre.api.commit.AlertType;
import gov.vha.isaac.ochre.api.commit.ChangeChecker;
import gov.vha.isaac.ochre.api.commit.CheckPhase;
import gov.vha.isaac.ochre.api.commit.CommitRecord;
import gov.vha.isaac.ochre.api.component.sememe.SememeChronology;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import gov.vha.isaac.ochre.collections.SememeSequenceSet;
import gov.vha.isaac.ochre.collections.SequenceSet;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;
import javafx.concurrent.Task;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.mahout.math.map.OpenIntIntHashMap;
import org.ihtsdo.otf.lookup.contracts.contracts.ActiveTaskSet;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.model.cc.concept.ConceptChronicle;
import org.ihtsdo.otf.tcc.model.version.Stamp;

/**
 *
 * @author kec
 */
public class CommitTask extends Task<Optional<CommitRecord>> {

    private static final Logger log = LogManager.getLogger();

    final String commitComment;
    final ConceptSequenceSet conceptsToCommit = new ConceptSequenceSet();
    final ConceptSequenceSet conceptsToCheck = new ConceptSequenceSet();
    final SememeSequenceSet sememesToCommit = new SememeSequenceSet();
    final SememeSequenceSet sememesToCheck = new SememeSequenceSet();
    final long lastCommit;
    private final ConcurrentSkipListSet<ChangeChecker> checkers;
    private final ConcurrentSkipListSet<Alert> alertCollection;
    private final Map<UncommittedStamp, Integer> pendingStampsForCommit;
    private final CommitProvider commitProvider;

    public CommitTask(String commitComment,
            ConceptSequenceSet uncommittedConceptsWithChecksSequenceSet,
            ConceptSequenceSet uncommittedConceptsNoChecksSequenceSet,
            SememeSequenceSet uncommittedSememesWithChecksSequenceSet,
            SememeSequenceSet uncommittedSememesNoChecksSequenceSet,
            long lastCommit,
            ConcurrentSkipListSet<ChangeChecker> checkers,
            ConcurrentSkipListSet<Alert> alertCollection,
            Map<UncommittedStamp, Integer> pendingStampsForCommit,
            CommitProvider commitProvider) {
        LookupService.getService(ActiveTaskSet.class).get().add(this);
        this.commitComment = commitComment;
        conceptsToCommit.or(uncommittedConceptsNoChecksSequenceSet);
        conceptsToCommit.or(uncommittedConceptsWithChecksSequenceSet);
        conceptsToCheck.or(uncommittedConceptsWithChecksSequenceSet);
        sememesToCommit.or(uncommittedSememesNoChecksSequenceSet);
        sememesToCommit.or(uncommittedSememesWithChecksSequenceSet);
        sememesToCheck.or(uncommittedSememesWithChecksSequenceSet);
        uncommittedConceptsNoChecksSequenceSet.clear();
        uncommittedConceptsWithChecksSequenceSet.clear();
        uncommittedSememesNoChecksSequenceSet.clear();
        uncommittedSememesWithChecksSequenceSet.clear();
        this.lastCommit = lastCommit;
        this.checkers = checkers;
        this.alertCollection = alertCollection;
        this.pendingStampsForCommit = pendingStampsForCommit;
        this.commitProvider = commitProvider;
        
    }

    @Override
    protected Optional<CommitRecord> call() throws Exception {
        try {
// TODO handle notification...
//            try {
//                GlobalPropertyChange.fireVetoableChange(TerminologyStoreDI.CONCEPT_EVENT.PRE_COMMIT, null, conceptsToCommit);
//            } catch (PropertyVetoException ex) {
//                return;
//            }
            conceptsToCommit.stream().forEach((conceptSequence) -> {
                try {
                    ConceptChronicle c = ConceptChronicle.get(
                            Get.identifierService().getConceptNid(conceptSequence));
                    c.modified(c.getConceptAttributes(), lastCommit);
                    if (conceptsToCheck.contains(conceptSequence)) {
                        checkers.stream().forEach((check) -> {
                            check.check(c, alertCollection, CheckPhase.COMMIT);
                        });
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            });
            sememesToCommit.stream().forEach((sememeSequence) -> {
                SememeChronology sc = Get.sememeService().getSememe(sememeSequence);
                if (sememesToCheck.contains(sememeSequence)) {
                    checkers.stream().forEach((check) -> {
                        check.check(sc, alertCollection, CheckPhase.COMMIT);
                    });
                }
            });
            if (alertCollection.stream().anyMatch((alert)
                    -> (alert.getAlertType() == AlertType.ERROR))) {
                commitProvider.revertCommit(conceptsToCommit, 
                        conceptsToCheck, 
                        sememesToCommit, 
                        sememesToCheck,
                        pendingStampsForCommit);
                return Optional.empty();
            }
            long commitTime = System.currentTimeMillis();
            SequenceSet stampSequenceSet = new SequenceSet();
            pendingStampsForCommit.entrySet().stream().forEach((entry) -> {
                int stampSequence = entry.getValue();
                stampSequenceSet.add(stampSequence);
                UncommittedStamp uncommittedStamp = entry.getKey();
                Stamp stamp = new Stamp(Status.getStatusFromState(entry.getKey().status),
                        commitTime,
                        Get.identifierService().getConceptNid(uncommittedStamp.authorSequence),
                        Get.identifierService().getConceptNid(uncommittedStamp.moduleSequence),
                        Get.identifierService().getConceptNid(uncommittedStamp.pathSequence));
                commitProvider.addStamp(stamp, stampSequence);
            });
            if (commitComment != null) {
                stampSequenceSet.stream().forEach((stamp)
                        -> commitProvider.addComment(stamp, commitComment));
            }
            CommitRecord commitRecord = null;
            if (!stampSequenceSet.isEmpty()) {
                commitRecord = new CommitRecord(Instant.ofEpochMilli(commitTime),
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
            //            if (indexers != null) {
//                for (IndexerBI i : indexers) {
//                    i.commitWriter();
//                }
//            }
//            GlobalPropertyChange.firePropertyChange(TerminologyStoreDI.CONCEPT_EVENT.POST_COMMIT, null, conceptsToCommit);
            return Optional.ofNullable(commitRecord);
        } catch (Exception e1) {
            throw new RuntimeException(e1);
        } finally {
           LookupService.getService(ActiveTaskSet.class).get().remove(this);
        }
    }

}
