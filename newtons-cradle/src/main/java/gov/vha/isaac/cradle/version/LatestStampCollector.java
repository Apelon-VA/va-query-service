/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.version;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiConsumer;
import org.ihtsdo.otf.tcc.model.version.RelativePosition;
import org.ihtsdo.otf.tcc.model.version.Stamp;

/**
 *
 * @author kec
 */
public class LatestStampCollector implements BiConsumer<LatestStampResult,StampedObject> {
    
    private final StampSequenceComputer computer;

    public LatestStampCollector(ViewPoint viewPoint) {
        this.computer = StampSequenceComputer.getComputer(viewPoint);
    }


    @Override
    public void accept(LatestStampResult latestStampResult, StampedObject possibleNewLatestStamp) {
        Set<StampedObject> oldResult = latestStampResult.getLatestStamps();
        latestStampResult.reset();
        if (oldResult.isEmpty()) {
            // Simple case, no results yet, just add the possible stamp...
            latestStampResult.add(possibleNewLatestStamp);
        } else if (oldResult.size() == 1) {
            // Only a single existing result (no contradiction identified), so see which is
            // latest, or if a contradiction exists. 
            StampedObject oldStampedObject = oldResult.iterator().next();
            RelativePosition relativePosition = computer.relativePosition(oldStampedObject.getStamp(), possibleNewLatestStamp.getStamp());
                    switch (relativePosition) {
                        case AFTER:
                            latestStampResult.add(oldStampedObject);
                            break;
                        case BEFORE:
                            latestStampResult.add(possibleNewLatestStamp);
                            break;
                        case CONTRADICTION:
                        case EQUAL:
                            latestStampResult.add(oldStampedObject);
                            if (!oldStampedObject.equals(possibleNewLatestStamp)) {
                                latestStampResult.add(possibleNewLatestStamp);
                            }
                            break;
                        case UNREACHABLE:
                            latestStampResult.addAll(oldResult);
                            if (computer.onRoute(possibleNewLatestStamp.getStamp())) {
                                latestStampResult.add(possibleNewLatestStamp);
                            }
                            break;
                        default: 
                            throw new UnsupportedOperationException("Can't handle: " + relativePosition);
                            
                    }
        } else {
            // Complicated case, current results contain at least one contradiction (size > 1)
            
            EnumSet<RelativePosition> relativePositions = EnumSet.noneOf(RelativePosition.class);
            oldResult.stream().forEach((oldResultStamp) -> {
                relativePositions.add(computer.relativePosition(possibleNewLatestStamp, oldResultStamp));
            });
            if (relativePositions.size() == 1) {
                switch ((RelativePosition) relativePositions.toArray()[0]) {
                        case AFTER:
                            latestStampResult.add(possibleNewLatestStamp);
                            break;
                        case BEFORE:
                        case UNREACHABLE:
                            oldResult.stream().forEach((oldResultStamp) -> {
                                latestStampResult.add(oldResultStamp);
                });
                            break;
                        case CONTRADICTION:
                        case EQUAL:
                            latestStampResult.add(possibleNewLatestStamp);
                            oldResult.stream().forEach((oldResultStamp) -> {
                                latestStampResult.add(oldResultStamp);
                });
                            break;
                            
                        default: 
                            throw new UnsupportedOperationException("Can't handle: " + relativePositions.toArray()[0]);                       
                }
            } else {
                oldResult.add(possibleNewLatestStamp);
                String stampInfo = Stamp.stampArrayToString(oldResult.stream().mapToInt((StampedObject value) -> value.getStamp()).toArray());
                System.out.println(stampInfo);
                throw new UnsupportedOperationException("Can't compute latest stamp for: " + stampInfo);
            }
        }
    }
}
