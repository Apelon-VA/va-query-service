package gov.vha.isaac.cradle.component;

import gov.vha.isaac.cradle.collections.CradleSerializer;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.model.version.Stamp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by kec on 12/18/14.
 */
public class StampSerializer implements CradleSerializer<Stamp>, Serializable {
    @Override
    public void serialize(DataOutput out, Stamp stamp) {
        try {
            out.writeBoolean(stamp.getStatus().getBoolean());
            out.writeLong(stamp.getTime());
            out.writeInt(stamp.getAuthorNid());
            out.writeInt(stamp.getModuleNid());
            out.writeInt(stamp.getPathNid());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Stamp deserialize(DataInput in) {
        try {
            return new Stamp(Status.getFromBoolean(in.readBoolean()),
                    in.readLong(), in.readInt(), in.readInt(), in.readInt());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

}
