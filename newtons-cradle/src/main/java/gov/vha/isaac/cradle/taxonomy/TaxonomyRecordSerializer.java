/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.taxonomy;

import gov.vha.isaac.cradle.waitfree.WaitFreeMergeSerializer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author kec
 */
public class TaxonomyRecordSerializer implements WaitFreeMergeSerializer<TaxonomyRecordPrimitive> {

    @Override
    public void serialize(DataOutput d, TaxonomyRecordPrimitive a) {
        if (a.unpacked != null) {
            a.taxonomyData = a.unpacked.pack();
        }
        try {
            if (a.taxonomyData.length > 0) {
                d.writeInt(a.taxonomyData.length);
                for (int i : a.taxonomyData) {
                    d.writeInt(i);
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public TaxonomyRecordPrimitive merge(TaxonomyRecordPrimitive a, TaxonomyRecordPrimitive b, long[] md5Data) {
        TaxonomyRecordUnpacked aRecords = a.getTaxonomyRecordUnpacked();
        TaxonomyRecordUnpacked bRecords = b.getTaxonomyRecordUnpacked();
        aRecords.merge(bRecords);
        return new TaxonomyRecordPrimitive(aRecords.pack(), md5Data);
    }

    @Override
    public TaxonomyRecordPrimitive deserialize(DataInput di, long[] md5Data) {
        try {
            int length = di.readInt();
            int[] result = new int[length];
            for (int i = 0; i < length; i++) {
                result[i] = di.readInt();
            }
            return new TaxonomyRecordPrimitive(result, md5Data);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

}
