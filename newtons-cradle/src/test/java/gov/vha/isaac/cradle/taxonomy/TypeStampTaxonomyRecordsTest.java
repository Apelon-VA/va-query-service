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
package gov.vha.isaac.cradle.taxonomy;

import gov.vha.isaac.cradle.taxonomy.TypeStampTaxonomyRecords.TypeStampTaxonomyRecord;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author kec
 */
public class TypeStampTaxonomyRecordsTest {
    
    public TypeStampTaxonomyRecordsTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of getIsaacDb method, of class TypeStampTaxonomyRecords.
     */
    @Test
    public void testTypeStampTaxonomyRecord() {
        System.out.println("testTypeStampTaxonomyRecord");
        TypeStampTaxonomyRecord record = new TypeStampTaxonomyRecord(1, 2, TaxonomyFlags.INFERRED.bits);
        assertEquals(1, record.getTypeSequence());
        assertEquals(2, record.getStamp());
        assertEquals(TaxonomyFlags.INFERRED.bits, record.getTaxonomyFlags());
        long recordAsLong = record.getAsLong();
        TypeStampTaxonomyRecord record2 = new TypeStampTaxonomyRecord(recordAsLong);
        assertEquals(1, record2.getTypeSequence());
        assertEquals(2, record2.getStamp());
        assertEquals(TaxonomyFlags.INFERRED.bits, record2.getTaxonomyFlags());
    }

  
    
}
