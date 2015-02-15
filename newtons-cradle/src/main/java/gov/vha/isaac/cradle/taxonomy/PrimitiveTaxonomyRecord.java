/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.taxonomy;

import gov.vha.isaac.cradle.CradleObject;
import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordUnpacked;


/**
 *
 * @author kec
 */
public class PrimitiveTaxonomyRecord implements CradleObject {

    long msb;
    long lsb;
    int[] array;
    transient TaxonomyRecordUnpacked unpacked = null;

    public PrimitiveTaxonomyRecord() {
        array = new int[0];
    }

    public PrimitiveTaxonomyRecord(int[] array, long[] md5Data) {
        this.msb = md5Data[0];
        this.lsb = md5Data[1];
        this.array = array;
    }

    public int[] getArray() {
        return array;
    }

    @Override
    public long getMd5Msb() {
        return msb;
    }

    @Override
    public long getMd5Lsb() {
        return lsb;
    }

    public TaxonomyRecordUnpacked getTaxonomyRecordUnpacked() {
        if (unpacked != null) {            
            return unpacked;
        }
        unpacked = new TaxonomyRecordUnpacked(array);
        return unpacked;
    }

    @Override
    public String toString() {
        return getTaxonomyRecordUnpacked().toString();
    }
}
