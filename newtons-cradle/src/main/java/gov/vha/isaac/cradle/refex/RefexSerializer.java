package gov.vha.isaac.cradle.refex;

import gov.vha.isaac.cradle.collections.CradleSerializer;
import gov.vha.isaac.cradle.component.ComponentModificationTracker;
import org.ihtsdo.otf.tcc.model.cc.refex.RefexGenericSerializer;
import org.ihtsdo.otf.tcc.model.cc.refex.RefexMember;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by kec on 7/28/14.
 */
public class RefexSerializer implements CradleSerializer<RefexMember<?,?>>, Serializable{

    @Override
    public void serialize(DataOutput out, RefexMember<?, ?> value) {
        try {
            RefexGenericSerializer.get().serialize(out, value);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public RefexMember<?, ?> deserialize(DataInput in) {
        try {
            return RefexGenericSerializer.get().deserialize(in, ComponentModificationTracker.get());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

}
