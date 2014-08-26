/*
 * Copyright 2013 International Health Terminology Standards Development Organisation.
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


import com.informatics.bdb.junit.ext.BdbTestRunner;
import com.informatics.bdb.junit.ext.BdbTestRunnerConfig;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.ihtsdo.otf.query.rest.server.AlternativeIdResource;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test converting UUIDs to SCTIDs and the reverse.
 *
 * @author kec
 */
@RunWith(BdbTestRunner.class)
@BdbTestRunnerConfig()
public class SctidUuidTest extends JerseyTest {

    @Override
    protected Application configure() {
        return new ResourceConfig(AlternativeIdResource.class);
    }

    public SctidUuidTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    @Override
    public void setUp() {
    }

    @After
    @Override
    public void tearDown() {
    }

    @Test
    public void runUuidTest() {
        String resultString = target("alternate-id/uuid/285649006").
                request(MediaType.TEXT_PLAIN).get(String.class);
        assertEquals("2b684fe1-8baf-34ef-9d2a-df03142c915a", resultString);
    }

    @Test
    public void runSctidTest() {
        String resultString = target("alternate-id/sctid/2b684fe1-8baf-34ef-9d2a-df03142c915a").
                request(MediaType.TEXT_PLAIN).get(String.class);
        assertEquals("285649006", resultString);

    }

    @Test
    public void runSctidTestRefset() {
        String resultString = target("alternate-id/uuid/447566000").request(MediaType.TEXT_PLAIN).get(String.class);
        assertEquals("c259d808-8011-3772-bece-b4fbde18d375", resultString);
    }

    @Test
    public void runNullSctidTest() {
        String resultString = target("alternate-id/sctid").request(MediaType.TEXT_PLAIN).get(String.class);
        assertEquals("Add the UUID to the end of the URL", resultString);
    }

    @Test
    public void runNullUUIDTest() {
        String resultString = target("alternate-id/uuid").request(MediaType.TEXT_PLAIN).get(String.class);
        assertEquals("Add the SNOMED ID to the end of the URL", resultString);
    }
}
