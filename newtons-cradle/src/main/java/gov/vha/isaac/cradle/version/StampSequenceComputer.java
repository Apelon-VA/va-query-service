/*
 * Copyright 2014 Informatics, Inc..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gov.vha.isaac.cradle.version;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.ochre.api.SequenceProvider;
import gov.vha.isaac.ochre.api.coordinate.StampCoordinate;
import gov.vha.isaac.ochre.collections.StampSequenceSet;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.mahout.math.map.OpenIntObjectHashMap;
import org.ihtsdo.otf.tcc.api.coordinate.Position;
import org.ihtsdo.otf.tcc.api.coordinate.Precedence;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.ihtsdo.otf.tcc.model.version.RelativePosition;

/**
 *
 * @author kec
 */
public class StampSequenceComputer {

    private static final ConcurrentHashMap<ViewPoint, StampSequenceComputer> sequenceComputerCache
            = new ConcurrentHashMap<>();
    SequenceProvider sequenceProvider = Hk2Looker.getService(SequenceProvider.class);

    
    //CradleExtensions termStore = Hk2Looker.getService(CradleExtensions.class);
    
    public static StampSequenceComputer getComputer(StampCoordinate stampCoordinate) {
        return getComputer(new ViewPoint(stampCoordinate));
    }


    public static StampSequenceComputer getComputer(ViewPoint viewPoint) {
        StampSequenceComputer pm = sequenceComputerCache.get(viewPoint);

        if (pm != null) {
            return pm;
        }

        pm = new StampSequenceComputer(viewPoint);

        StampSequenceComputer existing = sequenceComputerCache.putIfAbsent(viewPoint, pm);

        if (existing != null) {
            pm = existing;
        }

        return pm;
    }

    private final ViewPoint viewPoint;
    /**
     * Mapping from pathNid to each segment for that pathNid. There is one entry
     * for each path reachable antecedent to the destination position of the
     * computer.
     */
    private final OpenIntObjectHashMap<PathSegment> pathSequenceSegmentMap = new OpenIntObjectHashMap<>();

    public StampSequenceComputer(ViewPoint viewPoint) {
        this.viewPoint = viewPoint;
        setupPathSequenceSegmentMap(viewPoint);
    }

    private void setupPathSequenceSegmentMap(ViewPoint viewPoint) {
        // call to recursive method...
        addOriginsToPathNidSegmentMap(viewPoint.getPosition());
    }

    // recursively called method

    private BitSet addOriginsToPathNidSegmentMap(Position destination) {

        int pathNid = destination.getPath().getConceptNid();
        int pathSequence = sequenceProvider.getConceptSequence(pathNid);

        BitSet precedingSegments = new BitSet();
        destination.getOrigins().stream().forEach((origin) -> {
            int originPathNid = origin.getPath().getConceptNid();
            precedingSegments.set(sequenceProvider.getConceptSequence(originPathNid));
            // Recursive call
            precedingSegments.or(addOriginsToPathNidSegmentMap(origin));
        });
        PathSegment segment = new PathSegment(pathSequence,
                destination.getTime(), precedingSegments);
        pathSequenceSegmentMap.put(pathSequence, segment);
        return precedingSegments;
    }

    /**
     * Bypasses the onRoute test of <code>relativePosition</code>
     *
     * @param stamp1 the first part of the comparison.
     * @param stamp2 the second part of the comparison.
     * @param precedencePolicy
     * @return the <code>RelativePosition</code> of part1 compared to part2 with
     * respect to the destination position of the instances.
     */
    RelativePosition fastRelativePosition(int stamp1, int stamp2, Precedence precedencePolicy) {
        CradleExtensions idb = getIsaacDb();
        if (viewPoint.getActiveModuleNids().isEmpty() ||
                (viewPoint.getActiveModuleNids().contains(idb.getModuleNidForStamp(stamp1))
                && viewPoint.getActiveModuleNids().contains(idb.getModuleNidForStamp(stamp2)))) {
            int stamp1PathSequence = sequenceProvider.getConceptSequence(idb.getPathNidForStamp(stamp1));
            long stamp1Time = idb.getTimeForStamp(stamp1);
            int stamp2PathSequence = sequenceProvider.getConceptSequence(idb.getPathNidForStamp(stamp2));
            long stamp2Time = idb.getTimeForStamp(stamp2);
            
            if (stamp1PathSequence == stamp2PathSequence) {
                PathSegment seg = pathSequenceSegmentMap.get(stamp1PathSequence);
                if (seg.containsPosition(stamp1PathSequence, stamp1Time)
                        && seg.containsPosition(stamp2PathSequence, stamp2Time)) {
                    if (stamp1Time < stamp2Time) {
                        return RelativePosition.BEFORE;
                    }
                    if (stamp1Time > stamp2Time) {
                        return RelativePosition.AFTER;
                    }
                    if (stamp1Time == stamp2Time) {
                        return RelativePosition.EQUAL;
                    }
                }
                return RelativePosition.UNREACHABLE;
            }

            PathSegment seg1 = pathSequenceSegmentMap.get(stamp1PathSequence);
            PathSegment seg2 = pathSequenceSegmentMap.get(stamp2PathSequence);
            if (seg1 == null || seg2 == null) {
                return RelativePosition.UNREACHABLE;
            }
            if (!(seg1.containsPosition(stamp1PathSequence, stamp1Time)
                    && seg2.containsPosition(stamp2PathSequence, stamp2Time))) {
                return RelativePosition.UNREACHABLE;
            }
            if (precedencePolicy == Precedence.TIME) {
                if (stamp1Time < stamp2Time) {
                    return RelativePosition.BEFORE;
                }
                if (stamp1Time > stamp2Time) {
                    return RelativePosition.AFTER;
                }
                if (stamp1Time == stamp2Time) {
                    return RelativePosition.EQUAL;
                }
            }
            if (seg1.precedingSegments.get(stamp2PathSequence) == true) {
                return RelativePosition.BEFORE;
            }
            if (seg2.precedingSegments.get(stamp1PathSequence) == true) {
                return RelativePosition.AFTER;
            }
            return RelativePosition.CONTRADICTION;
        }
        return RelativePosition.UNREACHABLE;
    }

    public Position getDestination() {
        return viewPoint.getPosition();
    }

    public ViewPoint getViewPoint() {
        return viewPoint;
    }
    public boolean onRoute(StampedObject stampedObject) {
        return onRoute(stampedObject.getStamp());
    }
    /**
     *
     * @param stamp the part to be tested to determine if it is on route to the
     * destination.
     * @return true if the part's position is on the route to the destination of
     * the class's instance.
     */
    public boolean onRoute(int stamp) {
        int pathSequence = sequenceProvider.getConceptSequence(getIsaacDb().getPathNidForStamp(stamp));
        
        PathSegment seg = pathSequenceSegmentMap.get(pathSequence);
        if (seg != null) {
            return seg.containsPosition(pathSequence, getIsaacDb().getTimeForStamp(stamp));
        }
        return false;
    }

    public RelativePosition relativePosition(StampedObject stampedObject1, 
            StampedObject stampedObject2) {
        return relativePosition(stampedObject1.getStamp(), stampedObject2.getStamp());
    }
    /**
     *
     * @param stamp1 the first part of the comparison.
     * @param stamp2 the second part of the comparison.
     * @return the <code>RelativePosition</code> of v1 compared to v2 with
     * respect to the destination position of the class's instance.
     */
    public RelativePosition relativePosition(int stamp1, int stamp2) {
        if (!(onRoute(stamp1) && onRoute(stamp2))) {
            return RelativePosition.UNREACHABLE;
        }
        return fastRelativePosition(stamp1, stamp2, viewPoint.getPrecedencePolicy());
    }

    public IntStream getVisibleStamps(
            IntStream stamps) {
        return stamps.filter((stamp) -> onRoute(stamp));
        
    }
    
    
    private class LatestStampResultSupplier implements Supplier<LatestStampResult> {
        @Override
        public LatestStampResult get() {
            return new LatestStampResult();
        }
    };
    
    private class StampSequenceSetSupplier implements Supplier<StampSequenceSet> {
        @Override
        public StampSequenceSet get() {
            return new StampSequenceSet();
        }
    };
    
    public Set<StampedObject> getLatestStamps(
            Stream<StampedObject> stamps) {
        LatestStampResultSupplier supplier = new LatestStampResultSupplier();
        LatestStampCollector collector = new LatestStampCollector(this.viewPoint);
        LatestStampCombiner combiner = new LatestStampCombiner(this);
        LatestStampResult result = stamps.collect(supplier, 
                       collector, 
                       combiner);
        
        return result.getLatestStamps();
    }      
    
    public int[] getLatestStamps(IntStream stamps) {
        StampSequenceSetSupplier supplier = new StampSequenceSetSupplier();
        LatestPrimitiveStampCollector collector = new LatestPrimitiveStampCollector(this.viewPoint);
        LatestPrimitiveStampCombiner combiner = new LatestPrimitiveStampCombiner(this);
        StampSequenceSet result = stamps.collect(supplier, 
                       collector, 
                       combiner);
        
        return result.stream().toArray();
    }      
    

    /**
     * 
     * @param stamps A stream of stamps from which the latest is found, and then
     * tested to determine if the latest is active. 
     * @return true if any of the latest stamps (may be multiple in the case of a 
     * contradiction) are active. 
     */
    public boolean isLatestActive(IntStream stamps) {
        return Arrays.stream(getLatestStamps(stamps)).anyMatch((int stamp) -> 
                getIsaacDb().getStatusForStamp(stamp) == Status.ACTIVE);
    }
    
    private static CradleExtensions isaacDb;
    private static CradleExtensions getIsaacDb() {
        if (isaacDb == null) {
            isaacDb = Hk2Looker.getService(CradleExtensions.class);
        }
        return isaacDb;
    }
    
}
