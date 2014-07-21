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

import java.util.StringTokenizer;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import junit.framework.Assert;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.ihtsdo.otf.query.rest.server.DescriptionsForConceptResource;
import org.ihtsdo.otf.tcc.junit.BdbTestRunner;
import org.ihtsdo.otf.tcc.junit.BdbTestRunnerConfig;
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
public class DescriptionsForConceptTest extends JerseyTest {

    @Override
    protected Application configure() {
        return new ResourceConfig(DescriptionsForConceptResource.class);
    }

    public DescriptionsForConceptTest() {
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
    public void nullParamsTest() {
        String resultString = target("descriptions").
                request(MediaType.TEXT_PLAIN).get(String.class);
        Assert.assertEquals("Please enter SCTID.", resultString);
    }

    @Test
    public void getDescForConceptTest() {
        String resultString = target("descriptions/195500007").
                request(MediaType.TEXT_PLAIN).get(String.class);
        Assert.assertEquals(2, getResultCount(resultString));

    }

    @Test
    public void tooLongIDTest() {
        String resultString = target("descriptions/1955000070000000000000").
                request(MediaType.TEXT_PLAIN).get(String.class);
        Assert.assertEquals("Incorrect SNOMED id.", resultString);

    }

    public int getResultCount(String resultString) {
        int count = 0;
        StringTokenizer st = new StringTokenizer(resultString, "<>");
        while (st.hasMoreElements()) {
            String nextToken = st.nextToken();
            System.out.println(nextToken);
            if (nextToken.matches("Synonym.*") || nextToken.matches("Fully specified name") || nextToken.matches("Definition.*")) {
                count++;
            }
        }
        return count;
    }

}
