package gov.vha.isaac.cradle.refex;

import gov.vha.isaac.cradle.collections.CradleSerializer;
import gov.vha.isaac.cradle.component.ComponentModificationTracker;
import org.ihtsdo.otf.tcc.model.cc.refex.RefexGenericSerializer;
import org.ihtsdo.otf.tcc.model.cc.refex.RefexMember;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import org.ihtsdo.otf.tcc.api.chronicle.ComponentChronicleBI;
import org.ihtsdo.otf.tcc.model.cc.refexDynamic.RefexDynamicMember;
import org.ihtsdo.otf.tcc.model.cc.refexDynamic.RefexDynamicSerializer;

/**
 * Created by kec on 7/28/14.
 */
public class RefexSerializer implements CradleSerializer<ComponentChronicleBI<?>>, Serializable{
    
    private static final byte DYNAMIC = 0;
    private static final byte STANDARD = 1;

    @Override
    public void serialize(DataOutput out, ComponentChronicleBI<?> value) {
        try {
            if (value instanceof RefexDynamicMember) {
                out.writeByte(DYNAMIC);
                RefexDynamicSerializer.get().serialize(out, (RefexDynamicMember) value);
            } else if (value instanceof RefexMember) {
                out.writeByte(STANDARD);
                RefexGenericSerializer.get().serialize(out, (RefexMember<?, ?>) value);
            } else {
                throw new UnsupportedOperationException("Can't handle: " + value);
            }            
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public ComponentChronicleBI<?> deserialize(DataInput in) {
        try {
            switch (in.readByte()) {
                case DYNAMIC:
                    RefexDynamicMember member = new RefexDynamicMember();
                    RefexDynamicSerializer.get().deserialize(in, member);
                    return member;
                case STANDARD:
                    return RefexGenericSerializer.get().deserialize(in, ComponentModificationTracker.get());
                default:
                    throw new UnsupportedOperationException("Can't handle byte");
            }
            
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

}
