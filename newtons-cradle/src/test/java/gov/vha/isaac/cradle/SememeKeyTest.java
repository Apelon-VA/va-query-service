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
package gov.vha.isaac.cradle;

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
public class SememeKeyTest {
    
    public SememeKeyTest() {
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
     * Test of compareTo method, of class SememeKey.
     */
    @Test
    public void testCompareTo() {
        System.out.println("compareTo");
        SememeKey o = new SememeKey(1, 2, 3);
        SememeKey instance = new SememeKey(1, 2, 3);
        int expResult = 0;
        int result = instance.compareTo(o);
        assertEquals(expResult, result);
        instance = new SememeKey(4, 2, 3);
        expResult = 1;
        result = instance.compareTo(o);
        assertEquals(expResult, result);
        instance = new SememeKey(-1, 2, 3);
        expResult = -1;
        result = instance.compareTo(o);
        assertEquals(expResult, result);

        instance = new SememeKey(1, -2, 3);
        expResult = -1;
        result = instance.compareTo(o);
        assertEquals(expResult, result);
        
        instance = new SememeKey(1, 2, -3);
        expResult = -1;
        result = instance.compareTo(o);
        assertEquals(expResult, result);
    }

    /**
     * Test of equals method, of class SememeKey.
     */
    @Test
    public void testEquals() {
        System.out.println("equals");
        SememeKey o = new SememeKey(1, 2, 3);
        SememeKey instance = new SememeKey(1, 2, 3);
        boolean expResult = true;
        boolean result = instance.equals(o);
        assertEquals(expResult, result);
    }

    /**
     * Test of getKey1 method, of class SememeKey.
     */
    @Test
    public void testGetKey1() {
        System.out.println("getKey1");
        SememeKey instance = new SememeKey(1, 2, 3);
        int expResult = 1;
        int result = instance.getKey1();
        assertEquals(expResult, result);
    }

    /**
     * Test of getKey2 method, of class SememeKey.
     */
    @Test
    public void testGetKey2() {
        System.out.println("getKey2");
        SememeKey instance = new SememeKey(1, 2, 3);
        int expResult = 2;
        int result = instance.getKey2();
        assertEquals(expResult, result);
    }

    /**
     * Test of getSememeSequence method, of class SememeKey.
     */
    @Test
    public void testGetSememeSequence() {
        System.out.println("getSememeSequence");
        SememeKey instance = new SememeKey(1, 2, 3);
        int expResult = 3;
        int result = instance.getSememeSequence();
        assertEquals(expResult, result);
    }
    
}
