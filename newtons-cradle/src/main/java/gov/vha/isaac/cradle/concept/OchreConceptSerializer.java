package gov.vha.isaac.cradle.concept;

import gov.vha.isaac.cradle.waitfree.WaitFreeMergeSerializer;
import gov.vha.isaac.ochre.model.DataBuffer;
import gov.vha.isaac.ochre.model.concept.ConceptChronologyImpl;
import gov.vha.isaac.ochre.model.sememe.SememeChronologyImpl;

/**
 * Created by kec on 5/15/15.
 */
public class OchreConceptSerializer implements WaitFreeMergeSerializer<ConceptChronologyImpl> {

    @Override
    public void serialize(DataBuffer d, ConceptChronologyImpl a) {
        byte[] data = a.getDataToWrite();
        d.put(data, 0, data.length);
    }

    @Override
    public ConceptChronologyImpl merge(ConceptChronologyImpl a, ConceptChronologyImpl b, int writeSequence) {
        byte[] dataBytes = a.mergeData(writeSequence, b.getDataToWrite(writeSequence));
        DataBuffer db = new DataBuffer(dataBytes);
        return new ConceptChronologyImpl(db);
    }

    @Override
    public ConceptChronologyImpl deserialize(DataBuffer db) {
        return new ConceptChronologyImpl(db);
    }

}