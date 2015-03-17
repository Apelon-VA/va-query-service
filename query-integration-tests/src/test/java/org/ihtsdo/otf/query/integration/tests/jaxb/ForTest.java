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
package org.ihtsdo.otf.query.integration.tests.jaxb;

import java.io.IOException;

import org.ihtsdo.otf.query.implementation.ComponentCollectionTypes;
import org.ihtsdo.otf.query.implementation.ForSetSpecification;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.tcc.api.store.Ts;

import static org.testng.Assert.*;
import org.testng.annotations.*;

/**
 *
 * @author kec
 */
public class ForTest {

    public ForTest() {
    }

    @BeforeMethod
    public void setUp() {
    }

    @AfterMethod
    public void tearDown() {
    }

    @Test(groups = "QueryServiceTests")
    public void getAllComponentsTest() throws IOException {
        ForSetSpecification forCollection = new ForSetSpecification(ComponentCollectionTypes.ALL_COMPONENTS);
        NativeIdSetBI forSet = forCollection.getCollection();
        System.out.println(forSet.size());
        assertTrue(forSet.contiguous());
    }

    @Test(groups = "QueryServiceTests")
    public void getAllConceptstest() throws IOException {
        ForSetSpecification forCollection = new ForSetSpecification(ComponentCollectionTypes.ALL_CONCEPTS);
        NativeIdSetBI forSet = forCollection.getCollection();
        assertEquals(Ts.get().getConceptCount(), forSet.size());
    }
}
