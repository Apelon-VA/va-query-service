/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.version;

import java.util.Objects;
import org.apache.mahout.math.set.OpenIntHashSet;
import org.ihtsdo.otf.tcc.api.coordinate.Position;
import org.ihtsdo.otf.tcc.api.coordinate.Precedence;

/**
 *
 * @author kec
 */
public class ViewPoint {
    private final Position position;
    private final OpenIntHashSet activeModuleNids = new OpenIntHashSet();    
    private final Precedence precedencePolicy;
    
    public ViewPoint(Position position, OpenIntHashSet activeModuleSequencesParam,
            Precedence precedencePolicy) {
        this.position = new Position(position);
        activeModuleSequencesParam.forEachKey((int element) -> {
            activeModuleNids.add(element);
            return true;
        }); 
        this.precedencePolicy = precedencePolicy;
    }

    public Precedence getPrecedencePolicy() {
        return precedencePolicy;
    }

    public Position getPosition() {
        return position;
    }

    public OpenIntHashSet getActiveModuleNids() {
        return activeModuleNids;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 83 * hash + Objects.hashCode(this.position);
        hash = 83 * hash + Objects.hashCode(this.precedencePolicy);
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
        final ViewPoint other = (ViewPoint) obj;
        if (!Objects.equals(this.position, other.position)) {
            return false;
        }
        if (!Objects.equals(this.activeModuleNids, other.activeModuleNids)) {
            return false;
        }
        return this.precedencePolicy == other.precedencePolicy;
    }

    @Override
    public String toString() {
        return "ViewPoint{" + "position=" + position + ", activeModuleSequences=" + activeModuleNids + ", precedencePolicy=" + precedencePolicy + '}';
    }
}
