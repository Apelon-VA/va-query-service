/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.version;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author kec
 */
public class LatestStampResult {

    private final Set<StampedObject> latestStamps = new HashSet<>();

    public void addAll(Collection<StampedObject> stamps) {
        latestStamps.addAll(stamps);
    }
    
    public Set<StampedObject> getLatestStamps() {
        return new HashSet<>(latestStamps);
    }
    
    public void add(int stamp) {
        latestStamps.add(new StampedObjectWrapper(stamp));
    }
    
    public void add(StampedObject stampedObject) {
        latestStamps.add(stampedObject);
    }

    public void reset() {
        latestStamps.clear();
    }

}
