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
package gov.vha.isaac.cradle.concept;

import gov.vha.isaac.cradle.Cradle;
import gov.vha.isaac.cradle.builders.ConceptActiveService;
import gov.vha.isaac.cradle.collections.ConcurrentSequenceSerializedObjectMap;
import gov.vha.isaac.cradle.component.ConceptChronicleDataEager;
import gov.vha.isaac.cradle.component.ConceptChronicleDataEagerSerializer;
import gov.vha.isaac.ochre.api.DelegateService;
import gov.vha.isaac.ochre.api.IdentifierService;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.SystemStatusService;
import gov.vha.isaac.ochre.api.chronicle.StampedVersion;
import gov.vha.isaac.ochre.api.component.concept.ConceptChronology;
import gov.vha.isaac.ochre.api.component.concept.ConceptService;
import gov.vha.isaac.ochre.api.component.concept.ConceptSnapshot;
import gov.vha.isaac.ochre.api.component.concept.ConceptSnapshotService;
import gov.vha.isaac.ochre.api.component.concept.ConceptVersion;
import gov.vha.isaac.ochre.api.coordinate.StampCoordinate;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import gov.vha.isaac.ochre.model.concept.ConceptChronologyImpl;
import gov.vha.isaac.ochre.model.concept.ConceptSnapshotImpl;
import java.io.IOException;
import java.text.ParseException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.model.cc.concept.ConceptChronicle;

/**
 *
 * @author kec
 */
public class ConceptProviderOtfModel implements ConceptService, DelegateService {

    private static final Logger log = LogManager.getLogger();

    ConceptActiveService conceptActiveService;
    IdentifierService identifierProvider;

    final ConcurrentSequenceSerializedObjectMap<ConceptChronicleDataEager> conceptMap;
    private AtomicBoolean loadRequired = new AtomicBoolean();

    public ConceptProviderOtfModel() throws IOException, NumberFormatException, ParseException {
        try {
            log.info("Setting up OTF ConceptProvider at " + Cradle.getCradlePath().toAbsolutePath().toString());
            conceptMap = new ConcurrentSequenceSerializedObjectMap(new ConceptChronicleDataEagerSerializer(),
                    Cradle.getCradlePath(), "otf-concept-map/", ".otf-concepts.map");
        } catch (Exception e) {
            LookupService.getService(SystemStatusService.class).notifyServiceConfigurationFailure("ChRonicled Assertion Database of Logical Expressions (OTF)", e);
            throw e;
        }
    }

    @Override
    public void startDelegateService() {
        log.info("Starting OTF ConceptProvider post-construct");
        loadRequired.set(!Cradle.cradleStartedEmpty());
        conceptActiveService = LookupService.getService(ConceptActiveService.class);
        identifierProvider = LookupService.getService(IdentifierService.class);
        if (loadRequired.compareAndSet(true, false)) {

            log.info("Loading otf-concept-map.");
            conceptMap.read();

            log.info("Finished otf load.");
        }
    }

    @Override
    public void stopDelegateService()  {
        log.info("Stopping OTF ConceptProvider.");

        log.info("writing otf-concept-map.");
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

    public class ConceptSnapshotProvider implements ConceptSnapshotService {

        StampCoordinate stampCoordinate;

        public ConceptSnapshotProvider(StampCoordinate stampCoordinate) {
            this.stampCoordinate = stampCoordinate;
        }

        @Override
        public boolean isConceptActive(int conceptSequence) {
            return ConceptProviderOtfModel.this.isConceptActive(conceptSequence, stampCoordinate);
        }

        @Override
        public StampCoordinate getStampCoordinate() {
            return stampCoordinate;
        }

        @Override
        public ConceptSnapshot getConceptSnapshot(int conceptSequence) {
            return new ConceptSnapshotImpl((ConceptChronologyImpl) getConcept(conceptSequence), stampCoordinate);
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

    @Override
    public int getConceptCount() {
        return conceptMap.getSize();
    }

    public void writeConceptData(ConceptChronicleDataEager conceptData) {
        int sequence = identifierProvider.getConceptSequence(conceptData.getNid());
        conceptMap.put(sequence, conceptData);
        conceptData.setPrimordial(false);
    }

    @Override
    public void writeConcept(ConceptChronology<? extends ConceptVersion> concept) {
        ConceptChronicleDataEager conceptData = (ConceptChronicleDataEager) ((ConceptChronicle) concept).getData();
        writeConceptData(conceptData);
    }

    @Override
    public ConceptChronology getConcept(int conceptId) {
        try { 
            if (conceptId >= 0) {
                conceptId = identifierProvider.getConceptNid(conceptId);
            }
            return ConceptChronicle.get(conceptId);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public ConceptChronology<? extends StampedVersion> getConcept(UUID... conceptUuids) {
        return getConcept(identifierProvider.getNidForUuids(conceptUuids));
    }

    @Override
    public Stream<ConceptChronology<? extends ConceptVersion>> getConceptChronologyStream() {
        return getConceptStream().map((cc) -> {return (ConceptChronology<? extends ConceptVersion>) cc;});
    }

    @Override
    public Stream<ConceptChronology<? extends ConceptVersion>> getParallelConceptChronologyStream() {
        return getParallelConceptStream().map((cc) -> {return (ConceptChronology<? extends ConceptVersion>) cc;});
    }
    @Override
    public Stream<ConceptChronology<? extends ConceptVersion>> getParallelConceptChronologyStream(ConceptSequenceSet conceptSequences) {
        return conceptSequences.stream().parallel().mapToObj((int sequence) -> conceptMap.getQuick(sequence).getConceptChronicle());
    }

    @Override
    public Stream<ConceptChronology<? extends ConceptVersion>> getConceptChronologyStream(ConceptSequenceSet conceptSequences) {
        return conceptSequences.stream().mapToObj((int sequence) -> conceptMap.getQuick(sequence).getConceptChronicle());
    }


}
