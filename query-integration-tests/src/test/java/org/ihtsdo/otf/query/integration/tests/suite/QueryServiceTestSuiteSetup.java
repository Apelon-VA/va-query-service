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
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.embed.swing.JFXPanel;
import org.glassfish.hk2.runlevel.RunLevelController;
import org.ihtsdo.otf.query.integration.tests.QueryTest;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.datastore.BdbTerminologyStore;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.ihtsdo.otf.tcc.model.cc.termstore.PersistentStoreI;
import org.ihtsdo.otf.tcc.model.index.service.IndexerBI;
import org.testng.annotations.*;

/**
 * A test suite that establishes resources for use in tests that require the
 * setup of the {@link org.ihtsdo.otf.tcc.model.cc.termstore.PersistentStoreI}.
 *
 * @author dylangrald
 */
public class QueryServiceTestSuiteSetup {

    private static final String DIR = System.getProperty("user.dir");

    public static PersistentStoreI PS;
    private static final Logger LOGGER = Logger.getLogger(QueryServiceTestSuiteSetup.class.getName());

    @BeforeSuite
    public static void setUpSuite() {
        JFXPanel panel = new JFXPanel();
        LOGGER.log(Level.INFO, "oneTimeSetUp");
        System.setProperty(BdbTerminologyStore.BDB_LOCATION_PROPERTY, DIR + "/target/test-resources/berkeley-db");
        RunLevelController runLevelController = Hk2Looker.get().getService(RunLevelController.class);
        LOGGER.log(Level.INFO, "going to run level 1");
        runLevelController.proceedTo(1);
        LOGGER.log(Level.INFO, "going to run level 2");
        runLevelController.proceedTo(2);
        PS = Hk2Looker.get().getService(PersistentStoreI.class);

        ConceptChronicleBI concept;
        try {
            concept = PS.getConcept(UUID.fromString("2faa9260-8fb2-11db-b606-0800200c9a66"));
            LOGGER.log(Level.INFO, "WB concept: {0}", concept.toLongString());
        } catch (IOException ex) {
            Logger.getLogger(QueryTest.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    @AfterSuite
    public void tearDownSuite() throws Exception {
        LOGGER.log(Level.INFO, "oneTimeTearDown");
        RunLevelController runLevelController = Hk2Looker.get().getService(RunLevelController.class);
        LOGGER.log(Level.INFO, "going to run level 1");
        runLevelController.proceedTo(1);
        LOGGER.log(Level.INFO, "going to run level 0");
        runLevelController.proceedTo(0);
    }
}
