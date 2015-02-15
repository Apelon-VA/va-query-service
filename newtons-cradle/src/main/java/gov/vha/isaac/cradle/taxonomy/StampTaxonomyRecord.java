/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.taxonomy;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.cradle.version.StampedObject;
import java.io.IOException;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Objects;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;

/**
 *
 * @author kec
 */
public class StampTaxonomyRecord implements StampedObject {
    private static final CradleExtensions isaacDb = Hk2Looker.get().getService(CradleExtensions.class);

    int stamp;
    EnumSet<TaxonomyFlags> taxonomyFlags;

    
    public StampTaxonomyRecord(int stamp, EnumSet<TaxonomyFlags> taxonomyFlags) {
        this.stamp = stamp;
        this.taxonomyFlags = EnumSet.copyOf(taxonomyFlags);
    }

    @Override
    public int getStamp() {
        return stamp;
    }

    public EnumSet<TaxonomyFlags> getTaxonomyFlags() {
        return taxonomyFlags;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + this.stamp;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final StampTaxonomyRecord other = (StampTaxonomyRecord) obj;
        if (this.stamp != other.stamp) {
            return false;
        }
        return Objects.equals(this.taxonomyFlags, other.taxonomyFlags);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
             try {
                sb.append("«");
                sb.append(stamp);
                sb.append(" (s:");
                Status status = isaacDb.getStatusForStamp(stamp);
                sb.append(status);
                sb.append(" t:");
                Instant time = Instant.ofEpochMilli(isaacDb.getTimeForStamp(stamp));
                sb.append(time.toString());
                sb.append(" a:");
                ConceptChronicleBI author = isaacDb.getConcept(isaacDb.getAuthorNidForStamp(stamp));
                sb.append(author.toUserString());
                sb.append(" m:");
                ConceptChronicleBI module = isaacDb.getConcept(isaacDb.getModuleNidForStamp(stamp));
                sb.append(module.toUserString());
                sb.append(" p:");
                ConceptChronicleBI path = isaacDb.getConcept(isaacDb.getPathNidForStamp(stamp));
                sb.append(path.toUserString());
                sb.append(")->");
                sb.append(taxonomyFlags);
                sb.append("»");
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }

        return sb.toString();
    }    
}
