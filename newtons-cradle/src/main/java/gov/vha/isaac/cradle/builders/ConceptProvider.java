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

import gov.vha.isaac.cradle.Cradle;
import gov.vha.isaac.cradle.collections.ConcurrentSequenceSerializedObjectMap;
import gov.vha.isaac.cradle.component.ConceptChronicleDataEager;
import gov.vha.isaac.cradle.component.ConceptChronicleDataEagerSerializer;
import gov.vha.isaac.ochre.api.IdentifierService;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.SystemStatusService;
import gov.vha.isaac.ochre.api.chronicle.ChronicledConcept;
import gov.vha.isaac.ochre.api.concept.ConceptService;
import gov.vha.isaac.ochre.api.concept.ConceptSnapshotService;
import gov.vha.isaac.ochre.api.coordinate.StampCoordinate;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import java.io.IOException;
import java.text.ParseException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.glassfish.hk2.runlevel.RunLevel;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.model.cc.concept.ConceptChronicle;
import org.jvnet.hk2.annotations.Service;

/**
 *
 * @author kec
 */
@Service
@RunLevel(value = 1)

public class ConceptProvider implements ConceptService {

    private static final Logger log = LogManager.getLogger();

    ConceptActiveService conceptActiveService;
    IdentifierService identifierProvider;

    final ConcurrentSequenceSerializedObjectMap<ConceptChronicleDataEager> conceptMap;
    private AtomicBoolean loadRequired = new AtomicBoolean();

    //For HK2
    private ConceptProvider() throws IOException, NumberFormatException, ParseException {
        try {
            log.info("Setting up ConceptProvider at " + Cradle.getCradlePath().toAbsolutePath().toString());
            conceptMap = new ConcurrentSequenceSerializedObjectMap(new ConceptChronicleDataEagerSerializer(),
                    Cradle.getCradlePath(), "concept-map/", ".concepts.map");
        } catch (Exception e) {
            LookupService.getService(SystemStatusService.class).notifyServiceConfigurationFailure("ChRonicled Assertion Database of Logical Expressions", e);
            throw e;
        }
    }

    @PostConstruct
    private void startMe() throws IOException {
        log.info("Starting ConceptProvider post-construct");
        loadRequired.set(!Cradle.cradleStartedEmpty());
        conceptActiveService = LookupService.getService(ConceptActiveService.class);
        identifierProvider = LookupService.getService(IdentifierService.class);
        if (loadRequired.compareAndSet(true, false)) {

            log.info("Loading concept-map.");
            conceptMap.read();

            log.info("Finished load.");
        }
    }

    @PreDestroy
    private void stopMe() throws IOException {
        log.info("Stopping ConceptProvider.");

        log.info("writing concept-map.");
        conceptMap.write();
    }

    @Override
    public boolean isConceptActive(int conceptSequence, StampCoordinate stampCoordinate) {
        return conceptActiveService.isConceptActive(conceptSequence, stampCoordinate);
    }

    @Override
    public ConceptSnapshotService getSnapshot(StampCoordinate stampCoordinate) {
        return new ConceptSnapshotProvider(stampCoordinate);
    }

    @Override
    public ChronicledConcept getConcept(int conceptSequence) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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

    public Stream<ConceptChronicleDataEager> getConceptDataEagerStream() {
        return conceptMap.getStream();
    }

    public Stream<ConceptChronicleDataEager> getConceptDataEagerStream(ConceptSequenceSet conceptSequences) {
        return identifierProvider.getConceptSequenceStream()
                .filter((int sequence) -> conceptSequences.contains(sequence))
                .mapToObj((int sequence) -> {
                    Optional<ConceptChronicleDataEager> result = conceptMap.get(sequence);
                    if (result.isPresent()) {
                        return conceptMap.get(sequence).get();
                    }
                    throw new IllegalStateException("No concept for sequence: " + sequence);
                });

    }

    public Stream<ConceptChronicleDataEager> getParallelConceptDataEagerStream(ConceptSequenceSet conceptSequences) {
        return identifierProvider.getParallelConceptSequenceStream()
                .filter((int sequence) -> conceptSequences.contains(sequence))
                .mapToObj((int sequence) -> {
                    Optional<ConceptChronicleDataEager> result = conceptMap.get(sequence);
                    if (result.isPresent()) {
                        return conceptMap.get(sequence).get();
                    }
                    throw new IllegalStateException("No concept for sequence: " + sequence);
                });
    }

    public Stream<ConceptChronicleDataEager> getParallelConceptDataEagerStream() {
        return conceptMap.getParallelStream();
    }

    public Stream<ConceptChronicle> getConceptStream() {
        return conceptMap.getStream().map(ConceptChronicleDataEager::getConceptChronicle);
    }

    public Stream<ConceptChronicle> getParallelConceptStream() {
        return conceptMap.getParallelStream().map(ConceptChronicleDataEager::getConceptChronicle);
    }

    public Stream<? extends ConceptChronicleBI> getConceptStream(ConceptSequenceSet conceptSequences) throws IOException {
        return conceptSequences.stream().mapToObj((int sequence) -> conceptMap.getQuick(sequence).getConceptChronicle());
    }

    public Stream<? extends ConceptChronicleBI> getParallelConceptStream(ConceptSequenceSet conceptSequences) throws IOException {
        return conceptSequences.stream().parallel().mapToObj((int sequence) -> conceptMap.getQuick(sequence).getConceptChronicle());
    }

    public ConceptChronicleDataEager getConceptData(int i) throws IOException {
        if (i < 0) {
            i = identifierProvider.getConceptSequence(i);
        }
        Optional<ConceptChronicleDataEager> data = conceptMap.get(i);
        if (data.isPresent()) {
            return data.get();
        }
        return new ConceptChronicleDataEager(true);
    }

    public int getConceptCount() throws IOException {
        return conceptMap.getSize();
    }

    public void writeConceptData(ConceptChronicleDataEager conceptData) {
        int sequence = identifierProvider.getConceptSequence(conceptData.getNid());
        conceptMap.put(sequence, conceptData);
        conceptData.setPrimordial(false);
    }

}
