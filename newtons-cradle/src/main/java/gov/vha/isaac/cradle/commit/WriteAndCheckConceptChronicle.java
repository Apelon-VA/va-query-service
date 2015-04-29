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

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.cradle.component.ConceptChronicleDataEager;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.commit.Alert;
import gov.vha.isaac.ochre.api.commit.ChangeChecker;
import gov.vha.isaac.ochre.api.commit.ChangeListener;
import gov.vha.isaac.ochre.api.commit.CheckPhase;
import java.lang.ref.WeakReference;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Semaphore;
import javafx.concurrent.Task;
import org.ihtsdo.otf.lookup.contracts.contracts.ActiveTaskSet;
import org.ihtsdo.otf.tcc.model.cc.concept.ConceptChronicle;

/**
 *
 * @author kec
 */
public class WriteAndCheckConceptChronicle extends Task<Void> implements Callable<Void> {

    private static final CradleExtensions cradle = LookupService.getService(CradleExtensions.class);

    private final ConceptChronicle cc;
    private final ConcurrentSkipListSet<ChangeChecker> checkers;
    private final ConcurrentSkipListSet<Alert> alertCollection;
    private final Semaphore writeSemaphore;
    private final ConcurrentSkipListSet<WeakReference<ChangeListener>> changeListeners;

    public WriteAndCheckConceptChronicle(ConceptChronicle cc,
            ConcurrentSkipListSet<ChangeChecker> checkers,
            ConcurrentSkipListSet<Alert> alertCollection, Semaphore writeSemaphore,
            ConcurrentSkipListSet<WeakReference<ChangeListener>> changeListeners) {
        this.cc = cc;
        this.checkers = checkers;
        this.alertCollection = alertCollection;
        this.writeSemaphore = writeSemaphore;
        this.changeListeners = changeListeners;
        updateTitle("Write and check concept");
        updateMessage(cc.toUserString());
        updateProgress(-1, Long.MAX_VALUE); // Indeterminate progress
        LookupService.getService(ActiveTaskSet.class).get().add(this);
    }

    @Override
    public Void call() throws Exception {
        try {
            cradle.writeConceptData((ConceptChronicleDataEager) cc.getData());
            updateProgress(1, 3);
            updateMessage("checking: " + cc.toUserString());
            
            if (cc.isUncommitted()) {
                checkers.stream().forEach((check) -> {
                    check.check(cc, alertCollection, CheckPhase.ADD_UNCOMMITTED);
                });
            }

            cradle.writeConceptData((ConceptChronicleDataEager) cc.getData());
            updateProgress(2, 3);
            updateMessage("notifying: " + cc.toUserString());

             changeListeners.forEach((listenerRef) -> {
                ChangeListener listener = listenerRef.get();
                if (listener == null) {
                    changeListeners.remove(listenerRef);
                } else {
                    listener.handleChange(cc);
                }
             });

            updateProgress(3, 3);
            updateMessage("complete: " + cc.toUserString());
            
             return null;
        } finally {
            writeSemaphore.release();
            LookupService.getService(ActiveTaskSet.class).get().remove(this);
        }
    }
}
