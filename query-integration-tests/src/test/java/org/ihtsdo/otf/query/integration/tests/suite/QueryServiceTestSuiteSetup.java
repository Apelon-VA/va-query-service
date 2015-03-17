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
import javafx.embed.swing.JFXPanel;
import org.glassfish.hk2.runlevel.RunLevelController;
import static gov.vha.isaac.lookup.constants.Constants.CHRONICLE_COLLECTIONS_ROOT_LOCATION_PROPERTY;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.ObjectChronicleTaskServer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javafx.concurrent.Task;
import org.apache.logging.log4j.LogManager;
import org.ihtsdo.otf.lookup.contracts.contracts.ActiveTaskSet;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.api.refex.RefexChronicleBI;
import org.ihtsdo.otf.tcc.api.store.TerminologyStoreDI;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.jvnet.testing.hk2testng.HK2;
import org.reactfx.EventStreams;
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
        JFXPanel panel = new JFXPanel();
        log.info("oneTimeSetUp");
        System.setProperty(CHRONICLE_COLLECTIONS_ROOT_LOCATION_PROPERTY, "target/object-chronicles");

        
        java.nio.file.Path dbFolderPath = Paths.get(System.getProperty(CHRONICLE_COLLECTIONS_ROOT_LOCATION_PROPERTY));
        dbExists = dbFolderPath.toFile().exists();
        System.out.println("termstore folder path: " + dbFolderPath.toFile().exists());

        RunLevelController runLevelController = Hk2Looker.get().getService(RunLevelController.class);
        log.info("going to run level 1");
        runLevelController.proceedTo(1);
        log.info("going to run level 2");
        runLevelController.proceedTo(2);
        tickSubscription = EventStreams.ticks(Duration.ofSeconds(10))
                .subscribe(tick -> {
                    Set<Task> taskSet = Hk2Looker.get().getService(ActiveTaskSet.class).get();
                     taskSet.stream().forEach((task) -> {
                        double percentProgress = task.getProgress() * 100;
                        if (percentProgress < 0) {
                            percentProgress = 0;
                        }
                        log.printf(org.apache.logging.log4j.Level.INFO, "%n    %s%n    %s%n    %.1f%% complete",
                                task.getTitle(), task.getMessage(), percentProgress);
                    });
                });
        
        ObjectChronicleTaskServer tts = Hk2Looker.get().getService(ObjectChronicleTaskServer.class);
        TerminologyStoreDI store = Hk2Looker.get().getService(TerminologyStoreDI.class);
 
        if (!dbExists) {
            loadDatabase(tts);
         }
        
        ConceptChronicleBI concept;
        try {
            concept = store.getConcept(UUID.fromString("2faa9260-8fb2-11db-b606-0800200c9a66"));
            log.info("WB concept: {0}", concept.toLongString());

            concept = store.getConcept(UUID.fromString("45a8fde8-535d-3d2a-b76b-95ab67718b41"));

            log.info("SNOMED concept: {0}", concept.toLongString());

            for (RefexChronicleBI annotation: concept.getConceptAttributes().getAnnotations()) {
                log.info("Annotation concept nid: {0}, annotation: {1}",
                        new Object[] {Integer.toString(store.getConceptNidForNid(annotation.getNid())),
                                annotation.toString()});
            }

        } catch (IOException ex) {
            log.error(ex.getLocalizedMessage(), ex);
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        }

    }

      @AfterSuite
    public void tearDownSuite() throws Exception {
        log.info("oneTimeTearDown");
        RunLevelController runLevelController = Hk2Looker.get().getService(RunLevelController.class);
        log.info("going to run level 1");
        runLevelController.proceedTo(1);
        log.info("going to run level 0");
        runLevelController.proceedTo(0);
        log.info("going to run level -1");
        runLevelController.proceedTo(-1);
    }
    
    
    private void loadDatabase(ObjectChronicleTaskServer tts) throws InterruptedException, ExecutionException  {
        Path snomedDataFile = Paths.get("target/test-resources/sctSiEConcepts.jbin");
        Path logicMetadataFile = Paths.get("target/test-resources/isaac/metadata/econ/IsaacMetadataAuxiliary.econ");
        Instant start = Instant.now();

        Task<Integer> loadTask = tts.startLoadTask(IsaacMetadataAuxiliaryBinding.DEVELOPMENT,
                snomedDataFile, logicMetadataFile);
        Hk2Looker.get().getService(ActiveTaskSet.class).get().add(loadTask);
        int conceptCount = loadTask.get();
        Hk2Looker.get().getService(ActiveTaskSet.class).get().remove(loadTask);
        Instant finish = Instant.now();
        Duration duration = Duration.between(start, finish);
        log.info("  Loaded " + conceptCount + " concepts in: " + duration);
        double nsPerConcept = 1.0d * duration.toNanos() / conceptCount;
        log.info("  nsPerConcept: {}", nsPerConcept);

        double msPerConcept = 1.0d * duration.toMillis() / conceptCount;
        log.info("  msPerConcept: {}", msPerConcept);

    }

}
