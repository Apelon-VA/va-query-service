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

import gov.vha.isaac.cradle.builders.ConceptActiveService;
import gov.vha.isaac.cradle.collections.ConcurrentSequenceSerializedObjectMap;
import gov.vha.isaac.cradle.component.ConceptChronicleDataEager;
import gov.vha.isaac.cradle.component.ConceptChronicleDataEagerSerializer;
import gov.vha.isaac.ochre.api.ConfigurationService;
import gov.vha.isaac.ochre.api.DelegateService;
import gov.vha.isaac.ochre.api.Get;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.SystemStatusService;
import gov.vha.isaac.ochre.api.chronicle.LatestVersion;
import gov.vha.isaac.ochre.api.component.concept.ConceptChronology;
import gov.vha.isaac.ochre.api.component.concept.ConceptService;
import gov.vha.isaac.ochre.api.component.concept.ConceptSnapshot;
import gov.vha.isaac.ochre.api.component.concept.ConceptSnapshotService;
import gov.vha.isaac.ochre.api.component.concept.ConceptVersion;
import gov.vha.isaac.ochre.api.component.sememe.version.DescriptionSememe;
import gov.vha.isaac.ochre.api.coordinate.LanguageCoordinate;
import gov.vha.isaac.ochre.api.coordinate.StampCoordinate;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import gov.vha.isaac.ochre.model.concept.ConceptChronologyImpl;
import gov.vha.isaac.ochre.model.concept.ConceptSnapshotImpl;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

    final ConcurrentSequenceSerializedObjectMap<ConceptChronicleDataEager> conceptMap;
    private AtomicBoolean loadRequired = new AtomicBoolean();
    private final Path folderPath;

    public ConceptProviderOtfModel() throws IOException, NumberFormatException, ParseException {
        try {
            folderPath = LookupService.getService(ConfigurationService.class).getChronicleFolderPath().resolve("otf-concepts");
            loadRequired.set(!Files.exists(folderPath));
            Files.createDirectories(folderPath);
           log.info("Setting up OTF ConceptProvider at " + folderPath.toAbsolutePath().toString());
            conceptMap = new ConcurrentSequenceSerializedObjectMap(new ConceptChronicleDataEagerSerializer(),
                    folderPath, "otf-concept-map/", ".otf-concepts.map");
        } catch (Exception e) {
            LookupService.getService(SystemStatusService.class).notifyServiceConfigurationFailure("ChRonicled Assertion Database of Logical Expressions (OTF)", e);
            throw e;
        }
    }

    @Override
    public void startDelegateService() {
        log.info("Starting OTF ConceptProvider post-construct");
        conceptActiveService = LookupService.getService(ConceptActiveService.class);
        if (!loadRequired.get()) {

            log.info("Reading existing otf-concept-map.");
            conceptMap.read();

            log.info("Finished otf read.");
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
    public ConceptSnapshotService getSnapshot(StampCoordinate stampCoordinate, LanguageCoordinate languageCoordinate) {
       return new ConceptSnapshotProvider(stampCoordinate, languageCoordinate);
    }

    public class ConceptSnapshotProvider implements ConceptSnapshotService {

        StampCoordinate stampCoordinate;
        LanguageCoordinate languageCoordinate;
        
        public ConceptSnapshotProvider(StampCoordinate stampCoordinate, LanguageCoordinate languageCoordinate) {
            this.stampCoordinate = stampCoordinate;
            this.languageCoordinate = languageCoordinate;
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
            return new ConceptSnapshotImpl((ConceptChronologyImpl) getConcept(conceptSequence), stampCoordinate, languageCoordinate);
        }

        @Override
        public LanguageCoordinate getLanguageCoordinate() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Optional<LatestVersion<DescriptionSememe>> getFullySpecifiedDescription(int conceptId) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Optional<LatestVersion<DescriptionSememe>> getPreferredDescription(int conceptId) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

    
        @Override
        public Optional<LatestVersion<DescriptionSememe>> getDescriptionOptional(int conceptId) {
            conceptId = Get.identifierService().getConceptNid(conceptId);
            Optional<LatestVersion<DescriptionSememe>> fsd = getFullySpecifiedDescription(conceptId);
            if (fsd.isPresent()) {
                return fsd;
            }
            Optional<LatestVersion<DescriptionSememe>> pd = getPreferredDescription(conceptId);
            if (pd.isPresent()) {
                return pd;
            }
            return Optional.empty();
        }        

        @Override
        public String conceptDescriptionText(int conceptId) {
            Optional<LatestVersion<DescriptionSememe>> value = getDescriptionOptional(conceptId);
            if (value.isPresent()) {
                return value.get().value().getText();
            }
            return "No description for: " + conceptId;
        }
    }

    public Stream<ConceptChronicleDataEager> getConceptDataEagerStream() {
        return conceptMap.getStream();
    }

    public Stream<ConceptChronicleDataEager> getConceptDataEagerStream(ConceptSequenceSet conceptSequences) {
        return Get.identifierService().getConceptSequenceStream()
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
        return Get.identifierService().getParallelConceptSequenceStream()
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
            i = Get.identifierService().getConceptSequence(i);
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
        int sequence = Get.identifierService().getConceptSequence(conceptData.getNid());
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
                conceptId = Get.identifierService().getConceptNid(conceptId);
            }
            return ConceptChronicle.get(conceptId);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Optional<? extends ConceptChronology<? extends ConceptVersion>> getOptionalConcept(int conceptId) {
        try {
            if (conceptId >= 0) {
                conceptId = Get.identifierService().getConceptNid(conceptId);
            }
            return Optional.of(ConceptChronicle.get(conceptId));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Optional<? extends ConceptChronology<? extends ConceptVersion>> getOptionalConcept(UUID... conceptUuids) {
        return getOptionalConcept(Get.identifierService().getConceptSequenceForUuids(conceptUuids));
    }

    @Override
    public ConceptChronology<? extends ConceptVersion> getConcept(UUID... conceptUuids) {
        return getConcept(Get.identifierService().getNidForUuids(conceptUuids));
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

    @Override
    public ConceptService getDelegate() {
        return this;
    }
}
