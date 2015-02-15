package gov.vha.isaac.cradle;

import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordPacked;
import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordUnpacked;
import gov.vha.isaac.cradle.taxonomy.TaxonomyFlags;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TaxonomyRecordPackedTest {

    private static final int conceptSequence1 = 4;
    private static final int conceptSequence2 = 33;
    private static final int stamp1 = 5;
    private static final int stamp2 = 77;
    private static final int stamp2b = 79;
    private static final TaxonomyFlags[] flags1 = { TaxonomyFlags.STATED, TaxonomyFlags.PARENT};
    private static final TaxonomyFlags[] flags2 = { TaxonomyFlags.STATED, TaxonomyFlags.CHILD};
    private static final TaxonomyFlags[] flags2b = { TaxonomyFlags.INFERRED, TaxonomyFlags.CHILD};
    private TaxonomyRecordPacked testRecord1;
    private TaxonomyRecordPacked testRecord2;
    
    @Before
    public void setup() {
        testRecord1 = new TaxonomyRecordPacked(new int[]{0,0});
        testRecord1.setSequence(0, conceptSequence1);
        testRecord1.setStampAndFlags(1, stamp1, flags1);
        testRecord1.setConceptSequenceStampRecordLength(0, testRecord1.getTaxonomyData().length);

        testRecord2 = new TaxonomyRecordPacked(new int[]{0,0,0});
        testRecord2.setSequence(0, conceptSequence2);
        testRecord2.setStampAndFlags(1, stamp2, flags2);
        testRecord2.setStampAndFlags(2, stamp2b, flags2b);
        testRecord2.setConceptSequenceStampRecordLength(0, testRecord2.getTaxonomyData().length);
    
    }

    @Test
    public void testNextRecordIndex() throws Exception {
        Assert.assertEquals(2, testRecord1.nextRecordIndex(0));
        Assert.assertEquals(3, testRecord2.nextRecordIndex(0));
    }

    @Test
    public void testGetConceptSequence() throws Exception {
        Assert.assertEquals(conceptSequence1, testRecord1.getConceptSequence(0));
        Assert.assertEquals(conceptSequence2, testRecord2.getConceptSequence(0));

    }

    @Test
    public void testAddConceptSequenceStampRecord() throws Exception {
        TaxonomyRecordUnpacked unpacked1 = testRecord1.unpack();
        TaxonomyRecordUnpacked unpacked2 = testRecord2.unpack();
        
        unpacked1.merge(unpacked2);
        unpacked2.merge(unpacked1);
        Assert.assertNotEquals(unpacked2, testRecord1.unpack());
        Assert.assertNotEquals(unpacked1, testRecord1.unpack());
        Assert.assertNotEquals(unpacked1, testRecord2.unpack());
        Assert.assertNotEquals(unpacked1, testRecord1.unpack());
        Assert.assertEquals(unpacked1, unpacked2);
        Assert.assertArrayEquals(unpacked1.pack(), unpacked2.pack());
    }

    @Test
    public void testInferredFlag() throws Exception {
        Assert.assertFalse(testRecord1.inferredFlagSet(1));
        Assert.assertTrue(testRecord2.inferredFlagSet(2));
        Assert.assertFalse(testRecord2.inferredFlagSet(1));
    }

    @Test
    public void testStatedFlagSet() throws Exception {
        Assert.assertTrue(testRecord1.statedFlagSet(1));
        Assert.assertTrue(testRecord2.statedFlagSet(1));
        Assert.assertFalse(testRecord2.statedFlagSet(2));
    }

    @Test
    public void testGetStamp() throws Exception {
        Assert.assertEquals(stamp1, testRecord1.getStamp(1));
        Assert.assertEquals(stamp2, testRecord2.getStamp(1));
        Assert.assertEquals(stamp2b, testRecord2.getStamp(2));
    }


    @Test
    public void testChildOf() throws Exception {
        Assert.assertFalse(testRecord1.childFlagSet(1));
        Assert.assertTrue(testRecord2.childFlagSet(1));
        Assert.assertTrue(testRecord2.childFlagSet(2));
    }

    @Test
    public void testParentOf() throws Exception {
        Assert.assertTrue(testRecord1.parentFlagSet(1));
        Assert.assertFalse(testRecord2.parentFlagSet(1));
        Assert.assertFalse(testRecord2.parentFlagSet(2));
    }
}