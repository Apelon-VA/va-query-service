/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.taxonomy;

import gov.vha.isaac.cradle.waitfree.WaitFreeMergeSerializer;
import gov.vha.isaac.ochre.model.DataBuffer;

/**
 *
 * @author kec
 */
public class TaxonomyRecordSerializer implements WaitFreeMergeSerializer<TaxonomyRecordPrimitive> {

    @Override
    public void serialize(DataBuffer d, TaxonomyRecordPrimitive a) {
        d.putInt(a.writeSequence);
        if (a.unpacked != null) {
            a.taxonomyData = a.unpacked.pack();
        }
        d.putIntArray(a.taxonomyData);
    }

    @Override
    public TaxonomyRecordPrimitive merge(TaxonomyRecordPrimitive a, TaxonomyRecordPrimitive b, int writeSequence) {
        TaxonomyRecordUnpacked aRecords = a.getTaxonomyRecordUnpacked();
        TaxonomyRecordUnpacked bRecords = b.getTaxonomyRecordUnpacked();
        aRecords.merge(bRecords);
        return new TaxonomyRecordPrimitive(aRecords.pack(), writeSequence);
    }

    @Override
    public TaxonomyRecordPrimitive deserialize(DataBuffer di) {
        int writeSequence = di.getInt();
        int[] result = di.getIntArray();
        return new TaxonomyRecordPrimitive(result, writeSequence);
    }

}
