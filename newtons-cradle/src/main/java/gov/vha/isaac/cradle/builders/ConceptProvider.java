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
package gov.vha.isaac.cradle.builders;

import gov.vha.isaac.ochre.api.concept.ConceptService;
import gov.vha.isaac.ochre.api.concept.ConceptSnapshotService;
import gov.vha.isaac.ochre.api.coordinate.StampCoordinate;
import java.io.IOException;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.glassfish.hk2.runlevel.RunLevel;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.jvnet.hk2.annotations.Service;

/**
 *
 * @author kec
 */
@Service
@RunLevel(value = 2)

public class ConceptProvider implements ConceptService {
    private static final Logger log = LogManager.getLogger();

    ConceptActiveService conceptActiveService;
    
    @PostConstruct
    private void startMe() throws IOException {
        log.info("Starting ConceptProvider post-construct");    
        conceptActiveService = Hk2Looker.getService(ConceptActiveService.class);
    }

    @PreDestroy
    private void stopMe() throws IOException {
        log.info("Stopping ConceptProvider.");
    }
    @Override
    public boolean isConceptActive(int conceptSequence, StampCoordinate stampCoordinate) {
        return conceptActiveService.isConceptActive(conceptSequence, stampCoordinate);
    }

    @Override
    public ConceptSnapshotService getSnapshot(StampCoordinate stampCoordinate) {
        return new ConceptSnapshotProvider(stampCoordinate);
    }
    
    public class ConceptSnapshotProvider implements ConceptSnapshotService {
        StampCoordinate stampCoordinate;

        public ConceptSnapshotProvider(StampCoordinate stampCoordinate) {
            this.stampCoordinate = stampCoordinate;
        }

        @Override
        public boolean isConceptActive(int conceptSequence) {
            return ConceptProvider.this.isConceptActive(conceptSequence, stampCoordinate);
        }

        @Override
        public StampCoordinate getStampCoordinate() {
            return stampCoordinate;
        }
        
        
    }
}
