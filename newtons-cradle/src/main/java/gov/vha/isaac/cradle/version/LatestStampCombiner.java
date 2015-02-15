/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.version;

import java.util.Set;
import java.util.function.BiConsumer;
import org.ihtsdo.otf.tcc.model.version.RelativePosition;

/**
 *
 * @author kec
 */
public class LatestStampCombiner implements BiConsumer<LatestStampResult,LatestStampResult> {
    private final StampSequenceComputer computer;

    public LatestStampCombiner(StampSequenceComputer computer) {
        this.computer = computer;
    }

    @Override
    public void accept(LatestStampResult t, LatestStampResult u) {
        Set<StampedObject> tStampSet = t.getLatestStamps();
        Set<StampedObject> uStampSet = u.getLatestStamps();
        t.reset();
        if (tStampSet.size() == 1 && uStampSet.size() == 1) {
            
            StampedObject stamp1 = tStampSet.iterator().next();
            StampedObject stamp2 = uStampSet.iterator().next();
            RelativePosition relativePosition = computer.relativePosition(stamp1.getStamp(), stamp2.getStamp());
                    switch (relativePosition) {
                        case AFTER:
                            break;
                        case BEFORE:
                            t.add(stamp2);
                            break;
                        case CONTRADICTION:
                        case EQUAL:
                            if (stamp1 != stamp2) {
                                t.add(stamp1);
                                t.add(stamp2);
                            }
                            break;
                        case UNREACHABLE:
                            if (computer.onRoute(stamp2.getStamp())) {
                                t.add(stamp1);
                                t.add(stamp2);
                            }
                            break;
                        default: 
                            throw new UnsupportedOperationException("Can't handle: " + relativePosition);
                            
                    }            
            
        } else {
            tStampSet.addAll(uStampSet);
            t.reset();
            t.addAll(computer.getLatestStamps(tStampSet.stream()));            
        }
    }
    
}
