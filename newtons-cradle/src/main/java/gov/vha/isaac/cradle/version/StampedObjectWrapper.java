/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.version;

/**
 *
 * @author kec
 */
class StampedObjectWrapper implements StampedObject {
    final int stamp;

    public StampedObjectWrapper(int stamp) {
        this.stamp = stamp;
    }

    @Override
    public int getStamp() {
        return stamp;
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
        final StampedObjectWrapper other = (StampedObjectWrapper) obj;
        return this.stamp == other.stamp;
    }

    @Override
    public String toString() {
        return "StampedObjectWrapper{" + "stamp=" + stamp + '}';
    }
    
}
