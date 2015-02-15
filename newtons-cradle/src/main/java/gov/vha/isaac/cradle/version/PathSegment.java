/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.version;

import java.util.BitSet;

/**
 *
 * @author kec
 */
public class PathSegment {

    /**
     * The pathSequence of this segment. Each ancestor path to the position of the
     * computer gets it's own segment.
     */
    int pathSequence;
    /**
     * The end time of the position of the relative position computer. stamps
     * with times after the end time are not part of the path.
     */
    long endTime;

    BitSet precedingSegments;

    public PathSegment(int pathSequence, long endTime, BitSet precedingSegments) {
        assert pathSequence >= 0;
        this.pathSequence = pathSequence;
        this.endTime = endTime;
        this.precedingSegments = new BitSet(precedingSegments.size());
        this.precedingSegments.or(precedingSegments);
    }

    public boolean containsPosition(int pathSequence, long time) {
        if (this.pathSequence == pathSequence && time != Long.MIN_VALUE) {
            return time <= endTime;
        }
        return false;
    }

}
