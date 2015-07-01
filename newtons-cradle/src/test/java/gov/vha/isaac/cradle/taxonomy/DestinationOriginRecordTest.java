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

import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
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
public class DestinationOriginRecordTest {
    
    public DestinationOriginRecordTest() {
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
     * Test of getOriginSequence method, of class DestinationOriginRecord.
     */
    @Test
    public void testGetOriginSequence() {
        System.out.println("getOriginSequence");
        DestinationOriginRecord instance = new DestinationOriginRecord(7,8);
        int expResult = 8;
        int result = instance.getOriginSequence();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDestinationSequence method, of class DestinationOriginRecord.
     */
    @Test
    public void testGetDestinationSequence() {
        System.out.println("getDestinationSequence");
        DestinationOriginRecord instance = new DestinationOriginRecord(7,8);
        int expResult = 7;
        int result = instance.getDestinationSequence();
        assertEquals(expResult, result);
    }

    /**
     * Test of equals method, of class DestinationOriginRecord.
     */
    @Test
    public void testEquals() {
        System.out.println("equals");
        Object obj = null;
        DestinationOriginRecord instance = new DestinationOriginRecord(7,8);
        boolean expResult = false;
        boolean result = instance.equals(obj);
        assertEquals(expResult, result);
        obj = new DestinationOriginRecord(7,8);
        expResult = true;
        result = instance.equals(obj);
        assertEquals(expResult, result);
        obj = new DestinationOriginRecord(7,7);
        expResult = false;
        result = instance.equals(obj);
        assertEquals(expResult, result);
        obj = new DestinationOriginRecord(9,8);
        expResult = false;
        result = instance.equals(obj);
        assertEquals(expResult, result);

    }

    /**
     * Test of compareTo method, of class DestinationOriginRecord.
     */
    @Test
    public void testCompareTo() {
        System.out.println("compareTo");
        DestinationOriginRecord o = new DestinationOriginRecord(7,8);
        DestinationOriginRecord instance = new DestinationOriginRecord(7,8);
        int expResult = 0;
        int result = instance.compareTo(o);
        assertEquals(expResult, result);
        o = new DestinationOriginRecord(7,Integer.MIN_VALUE);
        expResult = 1;
        result = instance.compareTo(o);
        assertEquals(expResult, result);
        o = new DestinationOriginRecord(7,Integer.MAX_VALUE);
        expResult = -1;
        result = instance.compareTo(o);
        assertEquals(expResult, result);
    }
    
    @Test
    public void testCompareInNavigableSet() {
        ConcurrentSkipListSet<DestinationOriginRecord> destinationOriginRecordSet = new ConcurrentSkipListSet<>();       
        destinationOriginRecordSet.add(new DestinationOriginRecord(8,81));
        destinationOriginRecordSet.add(new DestinationOriginRecord(8,0));
        destinationOriginRecordSet.add(new DestinationOriginRecord(7,8));
        destinationOriginRecordSet.add(new DestinationOriginRecord(7,8));
        destinationOriginRecordSet.add(new DestinationOriginRecord(7,0));
        destinationOriginRecordSet.add(new DestinationOriginRecord(7,1));
        destinationOriginRecordSet.add(new DestinationOriginRecord(6,61));
        destinationOriginRecordSet.add(new DestinationOriginRecord(6,0));
        assertEquals(destinationOriginRecordSet.size(), 7);
        NavigableSet<DestinationOriginRecord> subset = destinationOriginRecordSet.subSet(new DestinationOriginRecord(7, Integer.MIN_VALUE), 
                                          new DestinationOriginRecord(7, Integer.MAX_VALUE));
        assertEquals(subset.size(), 3);
        
    }
    
    
    
}
