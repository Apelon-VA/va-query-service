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

import gov.vha.isaac.cradle.Cradle;
import gov.vha.isaac.cradle.builders.ConceptActiveService;
import gov.vha.isaac.cradle.waitfree.CasSequenceObjectMap;
import gov.vha.isaac.ochre.api.DelegateService;
import gov.vha.isaac.ochre.api.IdentifierService;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.SystemStatusService;
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
import java.nio.file.Path;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author kec
 */

public class ConceptProviderOchreModel implements ConceptService, DelegateService {

    private static final Logger log = LogManager.getLogger();

    ConceptActiveService conceptActiveService;
    IdentifierService identifierProvider;

    final CasSequenceObjectMap<ConceptChronologyImpl> conceptMap;
    private AtomicBoolean loadRequired = new AtomicBoolean();

    public ConceptProviderOchreModel() throws IOException, NumberFormatException, ParseException {
        try {
            log.info("Setting up OCHRE ConceptProvider at " + Cradle.getCradlePath().toAbsolutePath().toString());

            Path ochreConceptPath = Cradle.getCradlePath().resolve("ochre");

            conceptMap = new CasSequenceObjectMap(new OchreConceptSerializer(),
                    ochreConceptPath, "seg.", ".ochre-concepts.map");
        } catch (Exception e) {
            LookupService.getService(SystemStatusService.class).notifyServiceConfigurationFailure("ChRonicled Assertion Database of Logical Expressions (OCHRE)", e);
            throw e;
        }
    }

    @Override
    public void startDelegateService() {
        log.info("Starting OCHRE ConceptProvider post-construct");
        loadRequired.set(!Cradle.cradleStartedEmpty());
        conceptActiveService = LookupService.getService(ConceptActiveService.class);
        identifierProvider = LookupService.getService(IdentifierService.class);
        if (loadRequired.compareAndSet(true, false)) {

            log.info("Loading OCHRE concept-map.");
            conceptMap.initialize();

            log.info("Finished OCHRE load.");
        }
    }

    @Override
    public void stopDelegateService() {
        log.info("Stopping OCHRE ConceptProvider.");

        log.info("Writing OCHRE concept-map.");
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
    public ConceptChronologyImpl getConcept(int conceptSequence) {
        if (conceptSequence < 0) {
            conceptSequence = identifierProvider.getConceptSequence(conceptSequence);
        }
        return conceptMap.getQuick(conceptSequence);
    }

    @Override
    public ConceptChronologyImpl getConcept(UUID... conceptUuids) {
        int conceptNid = identifierProvider.getNidForUuids(conceptUuids);
        int conceptSequence = identifierProvider.getConceptSequence(conceptNid);
        Optional<ConceptChronologyImpl> optionalConcept = conceptMap.get(conceptSequence);
        if (optionalConcept.isPresent()) {
            return optionalConcept.get();
        }
        ConceptChronologyImpl concept = new ConceptChronologyImpl(conceptUuids[0], conceptNid, conceptSequence);
        if (conceptUuids.length > 1) {
            concept.setAdditionalUuids(Arrays.asList(Arrays.copyOfRange(conceptUuids, 1, conceptUuids.length)));
        }
        conceptMap.put(conceptSequence, concept);
        identifierProvider.setConceptSequenceForComponentNid(conceptSequence, conceptNid);
        return conceptMap.getQuick(conceptSequence);
    }

    public class ConceptSnapshotProvider implements ConceptSnapshotService {

        StampCoordinate stampCoordinate;

        public ConceptSnapshotProvider(StampCoordinate stampCoordinate) {
            this.stampCoordinate = stampCoordinate;
        }

        @Override
        public boolean isConceptActive(int conceptSequence) {
            return ConceptProviderOchreModel.this.isConceptActive(conceptSequence, stampCoordinate);
        }

        @Override
        public StampCoordinate getStampCoordinate() {
            return stampCoordinate;
        }

        @Override
        public ConceptSnapshot getConceptSnapshot(int conceptSequence) {
            return new ConceptSnapshotImpl(getConcept(conceptSequence), stampCoordinate);
        }
    }

    @Override
    public Stream<ConceptChronology<? extends ConceptVersion>> getConceptChronologyStream() {
        return conceptMap.getStream().map((cc) -> {return (ConceptChronology<? extends ConceptVersion>) cc;});
    }

    @Override
    public Stream<ConceptChronology<? extends ConceptVersion>> getConceptChronologyStream(ConceptSequenceSet conceptSequences) {
        return identifierProvider.getConceptSequenceStream()
                .filter((int sequence) -> conceptSequences.contains(sequence))
                .mapToObj((int sequence) -> {
                    Optional<ConceptChronologyImpl> result = conceptMap.get(sequence);
                    if (result.isPresent()) {
                        return conceptMap.get(sequence).get();
                    }
                    throw new IllegalStateException("No concept for sequence: " + sequence);
                });

    }

    @Override
    public Stream<ConceptChronology<? extends ConceptVersion>> getParallelConceptChronologyStream(ConceptSequenceSet conceptSequences) {
        return identifierProvider.getParallelConceptSequenceStream()
                .filter((int sequence) -> conceptSequences.contains(sequence))
                .mapToObj((int sequence) -> {
                    Optional<ConceptChronologyImpl> result = conceptMap.get(sequence);
                    if (result.isPresent()) {
                        return conceptMap.get(sequence).get();
                    }
                    throw new IllegalStateException("No concept for sequence: " + sequence);
                });
    }

    @Override
    public Stream<ConceptChronology<? extends ConceptVersion>> getParallelConceptChronologyStream() {
        return conceptMap.getParallelStream().map((cc) -> {return cc;});
    }


    public Optional<ConceptChronologyImpl> getConceptData(int i) throws IOException {
        if (i < 0) {
            i = identifierProvider.getConceptSequence(i);
        }
        return conceptMap.get(i);
    }

    @Override
    public int getConceptCount() {
        return conceptMap.getSize();
    }

    @Override
    public void writeConcept(ConceptChronology<? extends ConceptVersion> concept) {
        conceptMap.put(concept.getConceptSequence(), (ConceptChronologyImpl) concept);
    }

}
