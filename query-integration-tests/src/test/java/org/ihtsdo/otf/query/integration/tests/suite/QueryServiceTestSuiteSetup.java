/*
 * Copyright 2014 Informatics, Inc..
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
package org.ihtsdo.otf.query.integration.tests.suite;

import java.io.IOException;
import java.util.UUID;
import static gov.vha.isaac.ochre.api.constants.Constants.CHRONICLE_COLLECTIONS_ROOT_LOCATION_PROPERTY;
import static gov.vha.isaac.ochre.api.constants.Constants.SEARCH_ROOT_LOCATION_PROPERTY;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.ObjectChronicleTaskService;
import gov.vha.isaac.ochre.api.component.concept.ConceptChronology;
import gov.vha.isaac.ochre.api.component.concept.ConceptService;
import gov.vha.isaac.ochre.api.component.concept.ConceptServiceManagerI;
import gov.vha.isaac.ochre.api.memory.HeapUseTicker;
import gov.vha.isaac.ochre.api.progress.ActiveTasksTicker;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import javafx.concurrent.Task;
import org.apache.logging.log4j.LogManager;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.api.store.TerminologyStoreDI;
import org.jvnet.testing.hk2testng.HK2;
import org.reactfx.Subscription;
import org.testng.annotations.*;

/**
 * A test suite that establishes resources for use in tests that require the
 * setup of the {@link org.ihtsdo.otf.tcc.model.cc.termstore.PersistentStoreI}.
 *
 * @author dylangrald
 */
@HK2("query")
public class QueryServiceTestSuiteSetup {
    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger();
    Subscription tickSubscription;

   private boolean dbExists = false;

    @BeforeSuite
    public void setUpSuite() throws Exception {
        log.info("oneTimeSetUp");
        
        System.setProperty(CHRONICLE_COLLECTIONS_ROOT_LOCATION_PROPERTY, "target/object-chronicles");
        System.setProperty(SEARCH_ROOT_LOCATION_PROPERTY, "target/search");

        
        java.nio.file.Path dbFolderPath = Paths.get(System.getProperty(CHRONICLE_COLLECTIONS_ROOT_LOCATION_PROPERTY));
        dbExists = dbFolderPath.toFile().exists();
        System.out.println("termstore folder path: " + dbFolderPath.toFile().exists());

        LookupService.startupIsaac();
        ActiveTasksTicker.start(10);
        HeapUseTicker.start(10);
        
        ObjectChronicleTaskService tts = LookupService.getService(ObjectChronicleTaskService.class);
        ConceptService store = LookupService.getService(ConceptServiceManagerI.class).get();
 
        if (!dbExists) {
            loadDatabase(tts);
            indexDatabase(tts);
         }
        
        ConceptChronology concept;
        try {
            concept = store.getConcept(IsaacMetadataAuxiliaryBinding.ISAAC_ROOT.getUuids());
            log.info("Isaac Root concept: {0}", concept.toString());

            concept = store.getConcept(IsaacMetadataAuxiliaryBinding.HEALTH_CONCEPT.getUuids());

            log.info("Health concept: {0}", concept.toString());


        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        }

    }

      @AfterSuite
    public void tearDownSuite() throws Exception {
        log.info("oneTimeTearDown");
        ActiveTasksTicker.stop();
        HeapUseTicker.stop();
        LookupService.shutdownIsaac();
    }
    
    
    private void indexDatabase(ObjectChronicleTaskService tts) throws InterruptedException, ExecutionException {
        Instant start = Instant.now();

        Task<Void> indexTask = tts.startIndexTask();
        indexTask.get();
        Instant finish = Instant.now();
        Duration duration = Duration.between(start, finish);
        log.info("  Indexed db in: " + duration);
        

    }
    
    
    private void loadDatabase(ObjectChronicleTaskService tts) throws InterruptedException, ExecutionException  {
        Path snomedDataFile = Paths.get("target/test-resources/sctSiEConcepts.jbin");
        Path logicMetadataFile = Paths.get("target/test-resources/isaac/metadata/econ/IsaacMetadataAuxiliary.econ");
        Instant start = Instant.now();

        Task<Integer> loadTask = tts.startLoadTask(IsaacMetadataAuxiliaryBinding.DEVELOPMENT,
                snomedDataFile, logicMetadataFile);
        int conceptCount = loadTask.get();
        Instant finish = Instant.now();
        Duration duration = Duration.between(start, finish);
        log.info("  Loaded " + conceptCount + " concepts in: " + duration);
        double nsPerConcept = 1.0d * duration.toNanos() / conceptCount;
        log.info("  nsPerConcept: {}", nsPerConcept);

        double msPerConcept = 1.0d * duration.toMillis() / conceptCount;
        log.info("  msPerConcept: {}", msPerConcept);

    }

}
