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

import gov.vha.isaac.ochre.api.commit.Alert;
import gov.vha.isaac.ochre.api.commit.ChangeChecker;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ihtsdo.otf.tcc.model.cc.concept.ConceptChronicle;

/**
 *
 * @author kec
 */
public class WriteConceptCompletionService implements Runnable {
    private static final Logger log = LogManager.getLogger();
    private final ExecutorService writeConceptPool = Executors.newFixedThreadPool(2);

    ExecutorCompletionService<Void> conversionService = new ExecutorCompletionService(writeConceptPool);
    
    public void checkAndWrite(ConceptChronicle cc, 
            ConcurrentSkipListSet<ChangeChecker> checkers,
            ConcurrentSkipListSet<Alert> alertCollection) {
        conversionService.submit(new WriteAndCheckConceptChronicle(
                cc, checkers, alertCollection));
    }

    public void write(ConceptChronicle cc) {
        conversionService.submit(new WriteConceptChronicle(cc));
    }

    @Override
    public void run() {
        while (true) {
            try {
                Future<Void> future = conversionService.poll();
                future.get();
            } catch (InterruptedException ex) {
                log.warn(ex.getLocalizedMessage(), ex);
            } catch (ExecutionException ex) {
                log.error(ex.getLocalizedMessage(), ex);
            }
            
        }
    }
    
    public void cancel() {
        try {
            writeConceptPool.shutdown();
            writeConceptPool.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    
}
