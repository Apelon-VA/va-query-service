/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.version;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;
import javax.xml.bind.annotation.XmlAttribute;
import org.ihtsdo.otf.tcc.api.coordinate.ExternalStampBI;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.model.cc.termstore.Termstore;

/**
 *
 * @author kec
 */
public class StampUniversal implements ExternalStampBI {

    private static final long serialVersionUID = 1;
    @XmlAttribute
    public Status status;

    @XmlAttribute
    public long time;
    @XmlAttribute
    public UUID authorUuid;
    @XmlAttribute
    public UUID moduleUuid;
    @XmlAttribute
    public UUID pathUuid;

    public StampUniversal(DataInput in) throws IOException {
        this.status = Status.getFromBoolean(in.readBoolean());
        this.time = in.readLong();
        this.authorUuid = new UUID(in.readLong(), in.readLong());
        this.moduleUuid = new UUID(in.readLong(), in.readLong());
        this.pathUuid = new UUID(in.readLong(), in.readLong());
    }

    public StampUniversal(int stamp, Termstore store) throws IOException {
        this.status = store.getStatusForStamp(stamp);
        this.time = store.getTimeForStamp(stamp);
        this.authorUuid = store.getUuidPrimordialForNid(store.getAuthorNidForStamp(stamp));
        this.moduleUuid = store.getUuidPrimordialForNid(store.getModuleNidForStamp(stamp));
        this.pathUuid = store.getUuidPrimordialForNid(store.getPathNidForStamp(stamp));
    }

    public void writeExternal(DataOutput out) throws IOException {
        out.writeBoolean(this.status.getBoolean());
        out.writeLong(time);
        out.writeLong(this.authorUuid.getMostSignificantBits());
        out.writeLong(this.authorUuid.getLeastSignificantBits());
        out.writeLong(this.moduleUuid.getMostSignificantBits());
        out.writeLong(this.moduleUuid.getLeastSignificantBits());
        out.writeLong(this.pathUuid.getMostSignificantBits());
        out.writeLong(this.pathUuid.getLeastSignificantBits());
    }
    
    @Override
    public Status getStatus() {
        return status;
    }

    @Override
    public long getTime() {
        return time;
    }

    @Override
    public UUID getAuthorUuid() {
        return authorUuid;
    }

    @Override
    public UUID getModuleUuid() {
        return moduleUuid;
    }

    public UUID getPathUuid() {
        return pathUuid;
    }
}
