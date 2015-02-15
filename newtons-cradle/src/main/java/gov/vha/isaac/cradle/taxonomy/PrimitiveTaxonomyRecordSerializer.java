/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.taxonomy;

import gov.vha.isaac.cradle.collections.CradleMergeSerializer;
import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordUnpacked;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author kec
 */
public class PrimitiveTaxonomyRecordSerializer implements CradleMergeSerializer<PrimitiveTaxonomyRecord> {

    @Override
    public void serialize(DataOutput d, PrimitiveTaxonomyRecord a) {
        if (a.unpacked != null) {
            a.array = a.unpacked.pack();
        }
        try {
            if (a.array.length > 0) {
                d.writeInt(a.array.length);
                for (int i : a.array) {
                    d.writeInt(i);
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public PrimitiveTaxonomyRecord merge(PrimitiveTaxonomyRecord a, PrimitiveTaxonomyRecord b, long[] md5Data) {
        TaxonomyRecordUnpacked aRecords = a.getTaxonomyRecordUnpacked();
        TaxonomyRecordUnpacked bRecords = b.getTaxonomyRecordUnpacked();
        aRecords.merge(bRecords);
        return new PrimitiveTaxonomyRecord(aRecords.pack(), md5Data);
    }

    @Override
    public PrimitiveTaxonomyRecord deserialize(DataInput di, long[] md5Data) {
        try {
            int length = di.readInt();
            int[] result = new int[length];
            for (int i = 0; i < length; i++) {
                result[i] = di.readInt();
            }
            return new PrimitiveTaxonomyRecord(result, md5Data);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

}
