/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.collections;

import gov.vha.isaac.cradle.waitfree.WaitFreeMergeSerializer;
import gov.vha.isaac.ochre.model.DataBuffer;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.ihtsdo.otf.tcc.api.store.Ts;

/**
 *
 * @author kec
 */
public class SerializedAtomicReferenceArray extends AtomicReferenceArray<byte[]> {

    WaitFreeMergeSerializer isaacSerializer;

    int segment;

    public SerializedAtomicReferenceArray(int length, WaitFreeMergeSerializer isaacSerializer, int segment) {
        super(length);
        this.isaacSerializer = isaacSerializer;
        this.segment = segment;
    }

    /**
     * Returns the String representation of the current values of array.
     *
     * @return the String representation of the current values of array
     */
    @Override
    public String toString() {
        int iMax = length() - 1;
        if (iMax == -1) {
            return "≤≥";
        }

        StringBuilder b = new StringBuilder();
        for (int i = 0;; i++) {
            try {
                b.append('≤');
                int sequence = segment * length() + i;
                b.append(sequence);
                b.append(": ");
                b.append(Ts.get().getConcept(sequence));
                b.append(" ");
                byte[] byteData = get(i);
                if (byteData != null) {
                    DataBuffer db = new DataBuffer(byteData);
                    b.append(isaacSerializer.deserialize(db));
                } else {
                    b.append("null");
                }
                if (i == iMax) {
                    return b.append('≥').toString();
                }
                b.append('≥').append(' ');
            } catch (IOException ex) {
                Logger.getLogger(SerializedAtomicReferenceArray.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }


    public int getSegment() {
        return segment;
    }

}
