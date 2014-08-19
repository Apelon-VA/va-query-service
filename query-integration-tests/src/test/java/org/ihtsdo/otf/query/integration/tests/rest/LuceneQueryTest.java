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
package org.ihtsdo.otf.query.integration.tests.rest;

import com.informatics.bdb.junit.ext.BdbTestRunner;
import com.informatics.bdb.junit.ext.BdbTestRunnerConfig;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import static org.junit.Assert.*;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.ihtsdo.otf.query.rest.server.LuceneResource;
import org.ihtsdo.otf.tcc.api.nid.ConcurrentBitSet;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 *
 * @author dylangrald
 */
@RunWith(BdbTestRunner.class)
@BdbTestRunnerConfig()
public class LuceneQueryTest extends JerseyTest {

    public LuceneQueryTest() {
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

    @Override
    protected Application configure() {
        return new ResourceConfig(LuceneResource.class);
    }

    @Test
    public void nullLuceneTest() {
        String resultString = target("lucene").request(MediaType.TEXT_PLAIN).get(String.class);
        assertEquals("Put url encoded lucene query at the end of the url", resultString);
    }

    @Test
    public void testLucene() {
        String resultString = target("lucene/oligophrenia").request(MediaType.TEXT_PLAIN).get(String.class);
        System.out.println(resultString);
        NativeIdSetBI results = this.getNidSet(resultString);
        assertEquals(6, results.size());

    }

    public NativeIdSetBI getNidSet(String resultString) {
        NativeIdSetBI results = new ConcurrentBitSet();
        for (String s : resultString.split("<[/]*componentNid>")) {
            if (s.matches("[-]*[0-9]*")) {
                results.add(Integer.parseInt(s));
            }
        }
        return results;
    }
}
