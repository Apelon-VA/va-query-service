/*
 * Copyright 2015 U.S. Department of Veterans Affairs.
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
package gov.vha.isaac.cradle.concept;

import gov.vha.isaac.ochre.api.ConfigurationService;
import gov.vha.isaac.ochre.api.DelegateService;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.component.concept.ConceptChronology;
import gov.vha.isaac.ochre.api.component.concept.ConceptService;
import gov.vha.isaac.ochre.api.component.concept.ConceptSnapshotService;
import gov.vha.isaac.ochre.api.component.concept.ConceptVersion;
import gov.vha.isaac.ochre.api.coordinate.StampCoordinate;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import java.io.IOException;
import java.util.UUID;
import java.util.stream.Stream;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.glassfish.hk2.runlevel.RunLevel;
import org.jvnet.hk2.annotations.Service;

/**
 *
 * @author kec
 */
@Service
@RunLevel(value = 1)
public class ConceptProviderManager implements ConceptService {
    private static final Logger log = LogManager.getLogger();
    ConceptService delegate;

    @Override
    public ConceptChronology<? extends ConceptVersion> getConcept(int conceptSequence) {
        return delegate.getConcept(conceptSequence);
    }

    @Override
    public ConceptChronology<? extends ConceptVersion> getConcept(UUID... conceptUuids) {
        return delegate.getConcept(conceptUuids);
    }

    @Override
    public void writeConcept(ConceptChronology<? extends ConceptVersion> concept) {
        delegate.writeConcept(concept);
    }

    @Override
    public boolean isConceptActive(int conceptSequence, StampCoordinate stampCoordinate) {
        return delegate.isConceptActive(conceptSequence, stampCoordinate);
    }

    @Override
    public ConceptSnapshotService getSnapshot(StampCoordinate stampCoordinate) {
        return delegate.getSnapshot(stampCoordinate);
    }

    @Override
    public int getConceptCount() {
        return delegate.getConceptCount();
    }

    @Override
    public Stream<ConceptChronology<? extends ConceptVersion>> getConceptChronologyStream() {
        return delegate.getConceptChronologyStream();
    }

    @Override
    public Stream<ConceptChronology<? extends ConceptVersion>> getParallelConceptChronologyStream() {
        return delegate.getParallelConceptChronologyStream();
    }

    @Override
    public Stream<ConceptChronology<? extends ConceptVersion>> getConceptChronologyStream(ConceptSequenceSet conceptSequences) {
        return delegate.getConceptChronologyStream(conceptSequences);
    }

    @Override
    public Stream<ConceptChronology<? extends ConceptVersion>> getParallelConceptChronologyStream(ConceptSequenceSet conceptSequences) {
        return delegate.getParallelConceptChronologyStream(conceptSequences);
    }

    private ConceptProviderManager() throws Exception {
        //For HK2 only
        switch (LookupService.getService(ConfigurationService.class).getConceptModel()) {
            case OCHRE_CONCEPT_MODEL:
                delegate = new ConceptProviderOchreModel();
                break;

            case OTF_CONCEPT_MODEL:
                delegate = new ConceptProviderOtfModel();
                break;

            default:
                throw new UnsupportedOperationException("Can't handle: "
                        + LookupService.getService(ConfigurationService.class).getConceptModel());
        }
    }

    @Override
    public ConceptService getDelegate() {
        return delegate;
    }


    @PostConstruct
    private void startMe() throws IOException {
        log.info("Starting ConceptProvider.");
        ((DelegateService)delegate).startDelegateService();
    }
    @PreDestroy
    private void stopMe() throws IOException {
        log.info("Stopping ConceptProvider.");
        ((DelegateService)delegate).stopDelegateService();
    }
}
