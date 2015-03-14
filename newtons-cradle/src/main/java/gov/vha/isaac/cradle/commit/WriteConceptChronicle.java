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
import java.util.concurrent.Callable;
import javafx.concurrent.Task;
import org.ihtsdo.otf.lookup.contracts.contracts.ActiveTaskSet;
import org.ihtsdo.otf.tcc.model.cc.concept.ConceptChronicle;

/**
 *
 * @author kec
 */
public class WriteConceptChronicle extends Task<Void>  implements Callable<Void>{
    
    private static final CradleExtensions cradle = LookupService.getService(CradleExtensions.class);
    
    private final ConceptChronicle cc;

    public WriteConceptChronicle(ConceptChronicle cc) {
        this.cc = cc;
        updateTitle("Write concept");
        updateMessage(cc.toUserString());
        updateProgress(-1, Long.MAX_VALUE); // Indeterminate progress
        LookupService.getService(ActiveTaskSet.class).get().add(this);
    }

    @Override
    public Void call() throws Exception {
        try {
            cradle.writeConceptData((ConceptChronicleDataEager) cc.getData());
            updateProgress(1, 1); 
            return null;
        } finally {
            LookupService.getService(ActiveTaskSet.class).get().remove(this);            
        }
    }
    
}
