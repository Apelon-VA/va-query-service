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

import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.sememe.SememeChronicle;
import gov.vha.isaac.ochre.api.sememe.SememeService;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import javafx.concurrent.Task;
import org.ihtsdo.otf.lookup.contracts.contracts.ActiveTaskSet;

/**
 *
 * @author kec
 */
public class WriteSememeChronicle extends Task<Void>  implements Callable<Void>{
    
    private static final SememeService sememeService = LookupService.getService(SememeService.class);
    
    private final SememeChronicle sc;
    private final Semaphore writeSemaphore;

    public WriteSememeChronicle(SememeChronicle sc, Semaphore writeSemaphore) {
        this.sc = sc;
        this.writeSemaphore = writeSemaphore;
        updateTitle("Write sememe");
        updateMessage(sc.toUserString());
        updateProgress(-1, Long.MAX_VALUE); // Indeterminate progress
        LookupService.getService(ActiveTaskSet.class).get().add(this);
    }

    @Override
    public Void call() throws Exception {
        try {
            sememeService.writeSememe(sc);
            updateProgress(1, 1); 
            return null;
        } finally {
            writeSemaphore.release();
            LookupService.getService(ActiveTaskSet.class).get().remove(this);            
        }
    }
}