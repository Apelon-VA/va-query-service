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

import gov.vha.isaac.cradle.builders.ConceptActiveService;
import gov.vha.isaac.cradle.waitfree.CasSequenceObjectMap;
import gov.vha.isaac.ochre.api.ConceptModel;
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
import gov.vha.isaac.ochre.api.component.sememe.SememeChronology;
import gov.vha.isaac.ochre.api.component.sememe.version.DescriptionSememe;
import gov.vha.isaac.ochre.api.coordinate.LanguageCoordinate;
import gov.vha.isaac.ochre.api.coordinate.StampCoordinate;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import gov.vha.isaac.ochre.model.concept.ConceptChronologyImpl;
import gov.vha.isaac.ochre.model.concept.ConceptSnapshotImpl;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author kec
 */
public class ConceptProviderOchreModel implements ConceptService, DelegateService {

    private static final Logger log = LogManager.getLogger();
    public static final String CRADLE_PROPERTIES_FILE_NAME = "cradle.properties";
    public static final String CRADLE_DATA_VERSION = "1.5";
    public static final String CRADLE_DATA_VERSION_PROPERTY = "cradle.data.version";
    public static final String CRADLE_CONCEPT_MODEL_PROPERTY = "cradle.concept.model";

    ConceptActiveService conceptActiveService;

    final CasSequenceObjectMap<ConceptChronologyImpl> conceptMap;
    private AtomicBoolean loadRequired = new AtomicBoolean();

    public ConceptProviderOchreModel() throws IOException, NumberFormatException, ParseException {
        try {
            Path propertiesPath = LookupService.getService(ConfigurationService.class).getChronicleFolderPath().resolve(CRADLE_PROPERTIES_FILE_NAME);
            Path folderPath = LookupService.getService(ConfigurationService.class).getChronicleFolderPath().resolve("ochre-concepts");
            Files.createDirectories(folderPath);
            log.info("Setting up OCHRE ConceptProvider at " + folderPath.toAbsolutePath());
            Properties cradleProps = new Properties();
            if (propertiesPath.toFile().exists()) {
                try (FileInputStream in = new FileInputStream(propertiesPath.toFile())) {
                    cradleProps.load(in);
                }
                if (!cradleProps.getProperty(CRADLE_DATA_VERSION_PROPERTY).equals(CRADLE_DATA_VERSION)) {
                    throw new IllegalStateException("Unsupported data version: " + cradleProps);
                }
                ConceptModel model = ConceptModel.valueOf(cradleProps.getProperty(CRADLE_CONCEPT_MODEL_PROPERTY));
                ConceptModel expectedModel = LookupService.getService(ConfigurationService.class).getConceptModel();
                if (expectedModel != model) {
                    throw new IllegalStateException("Incompatible concept model in existing database. Expected: "
                            + expectedModel + " found: " + model + ".\n" + cradleProps);
                }
            } else {
                loadRequired.set(true);
                cradleProps.put(CRADLE_DATA_VERSION_PROPERTY, CRADLE_DATA_VERSION);
                cradleProps.put(CRADLE_CONCEPT_MODEL_PROPERTY, ConceptModel.OCHRE_CONCEPT_MODEL.name());
                try (FileOutputStream out = new FileOutputStream(propertiesPath.toFile())) {
                    cradleProps.store(out, CRADLE_DATA_VERSION);
                }
            }

            Path ochreConceptPath = folderPath.resolve("ochre");

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
        conceptActiveService = LookupService.getService(ConceptActiveService.class);
        if (!loadRequired.compareAndSet(true, false)) {

            log.info("Reading existing OCHRE concept-map.");
            conceptMap.initialize();

            log.info("Finished OCHRE read.");
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
    public ConceptSnapshotService getSnapshot(StampCoordinate stampCoordinate, LanguageCoordinate languageCoordinate) {
        return new ConceptSnapshotProvider(stampCoordinate, languageCoordinate);
    }

    @Override
    public Optional<? extends ConceptChronology<? extends ConceptVersion<?>>> getOptionalConcept(int conceptId) {
        if (conceptId < 0) {
            conceptId = Get.identifierService().getConceptSequence(conceptId);
        }
        return conceptMap.getOptional(conceptId);
    }

    @Override
    public Optional<? extends ConceptChronology<? extends ConceptVersion<?>>> getOptionalConcept(UUID... conceptUuids) {
        return getOptionalConcept(Get.identifierService().getConceptSequenceForUuids(conceptUuids));
    }

    @Override
    public ConceptChronologyImpl getConcept(int conceptId) {
        if (conceptId < 0) {
            conceptId = Get.identifierService().getConceptSequence(conceptId);
        }
        return conceptMap.getQuick(conceptId);
    }

    @Override
    public ConceptChronologyImpl getConcept(UUID... conceptUuids) {
        int conceptNid = Get.identifierService().getNidForUuids(conceptUuids);
        int conceptSequence = Get.identifierService().getConceptSequence(conceptNid);
        Optional<ConceptChronologyImpl> optionalConcept = conceptMap.get(conceptSequence);
        if (optionalConcept.isPresent()) {
            return optionalConcept.get();
        }
        ConceptChronologyImpl concept = new ConceptChronologyImpl(conceptUuids[0], conceptNid, conceptSequence);
        if (conceptUuids.length > 1) {
            concept.setAdditionalUuids(Arrays.asList(Arrays.copyOfRange(conceptUuids, 1, conceptUuids.length)));
        }
        conceptMap.put(conceptSequence, concept);
        Get.identifierService().setConceptSequenceForComponentNid(conceptSequence, conceptNid);
        return conceptMap.getQuick(conceptSequence);
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
            return ConceptProviderOchreModel.this.isConceptActive(conceptSequence, stampCoordinate);
        }

        @Override
        public StampCoordinate getStampCoordinate() {
            return stampCoordinate;
        }

        @Override
        public ConceptSnapshot getConceptSnapshot(int conceptSequence) {
            return new ConceptSnapshotImpl(getConcept(conceptSequence), stampCoordinate, languageCoordinate);
        }

        @Override
        public LanguageCoordinate getLanguageCoordinate() {
            return languageCoordinate;
        }

        @Override
        public Optional<LatestVersion<DescriptionSememe>> getFullySpecifiedDescription(int conceptId) {
            return languageCoordinate.getFullySpecifiedDescription(getDescriptionList(conceptId), stampCoordinate);
        }

        @Override
        public Optional<LatestVersion<DescriptionSememe>> getPreferredDescription(int conceptId) {
            return languageCoordinate.getPreferredDescription(getDescriptionList(conceptId), stampCoordinate);
        }

        private List<SememeChronology<DescriptionSememe>> getDescriptionList(int conceptId) {
            conceptId = Get.identifierService().getConceptNid(conceptId);
            return Get.sememeService().getDescriptionsForComponent(conceptId).collect(Collectors.toList());
        }

        @Override
        public Optional<LatestVersion<DescriptionSememe>> getDescriptionOptional(int conceptId) {
            return languageCoordinate.getDescription(getDescriptionList(conceptId), stampCoordinate);
        }

        @Override
        public String conceptDescriptionText(int conceptId) {
            Optional<LatestVersion<DescriptionSememe>> descriptionOptional
                    = getDescriptionOptional(conceptId);
            if (descriptionOptional.isPresent()) {
                return descriptionOptional.get().value().getText();
            }
            return "No desc for: " + conceptId;
        }

    }

    @Override
    public Stream<ConceptChronology<? extends ConceptVersion<?>>> getConceptChronologyStream() {
        return conceptMap.getStream().map((cc) -> {
            return (ConceptChronology<? extends ConceptVersion<?>>) cc;
        });
    }

    @Override
    public Stream<ConceptChronology<? extends ConceptVersion<?>>> getConceptChronologyStream(ConceptSequenceSet conceptSequences) {
        return Get.identifierService().getConceptSequenceStream()
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
    public Stream<ConceptChronology<? extends ConceptVersion<?>>> getParallelConceptChronologyStream(ConceptSequenceSet conceptSequences) {
        return Get.identifierService().getParallelConceptSequenceStream()
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
    public Stream<ConceptChronology<? extends ConceptVersion<?>>> getParallelConceptChronologyStream() {
        return conceptMap.getParallelStream().map((cc) -> {
            return cc;
        });
    }

    public Optional<ConceptChronologyImpl> getConceptData(int i) throws IOException {
        if (i < 0) {
            i = Get.identifierService().getConceptSequence(i);
        }
        return conceptMap.get(i);
    }

    @Override
    public int getConceptCount() {
        return conceptMap.getSize();
    }

    @Override
    public void writeConcept(ConceptChronology<? extends ConceptVersion<?>> concept) {
        conceptMap.put(concept.getConceptSequence(), (ConceptChronologyImpl) concept);
    }

    @Override
    public ConceptService getDelegate() {
        return this;
    }

}
