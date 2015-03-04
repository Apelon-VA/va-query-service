/*
 * Copyright 2015 kec.
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

import java.util.EnumSet;
import java.util.function.ObjIntConsumer;
import org.apache.mahout.math.set.OpenIntHashSet;
import org.ihtsdo.otf.tcc.model.version.RelativePosition;
import org.ihtsdo.otf.tcc.model.version.Stamp;

/**
 *
 * @author kec
 */
public class LatestPrimitiveStampCollector implements ObjIntConsumer<LatestStampsAsIntArray> {

    private final StampSequenceComputer computer;

    public LatestPrimitiveStampCollector(ViewPoint viewPoint) {
        this.computer = StampSequenceComputer.getComputer(viewPoint);
    }

    @Override
    public void accept(LatestStampsAsIntArray latestResult, int possibleNewLatestStamp) {
        OpenIntHashSet oldResult = latestResult.getLatestStamps();
        latestResult.reset();
        if (oldResult.isEmpty()) {
            // Simple case, no results yet, just add the possible stamp...
            oldResult.add(possibleNewLatestStamp);
        } else if (oldResult.size() == 1) {
            // Only a single existing result (no contradiction identified), so see which is
            // latest, or if a contradiction exists. 
            int oldStampedObject = oldResult.keys().get(0);
            RelativePosition relativePosition = computer.relativePosition(oldStampedObject, possibleNewLatestStamp);
            switch (relativePosition) {
                case AFTER:
                    latestResult.add(oldStampedObject);
                    break;
                case BEFORE:
                    latestResult.add(possibleNewLatestStamp);
                    break;
                case CONTRADICTION:
                case EQUAL:
                    latestResult.add(oldStampedObject);
                    if (oldStampedObject != possibleNewLatestStamp) {
                        latestResult.add(possibleNewLatestStamp);
                    }
                    break;
                case UNREACHABLE:
                    latestResult.addAll(oldResult);
                    if (computer.onRoute(possibleNewLatestStamp)) {
                        latestResult.add(possibleNewLatestStamp);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Can't handle: " + relativePosition);

            }
        } else {
            // Complicated case, current results contain at least one contradiction (size > 1)

            EnumSet<RelativePosition> relativePositions = EnumSet.noneOf(RelativePosition.class);
            oldResult.keys().forEach((oldResultStamp) -> {
                relativePositions.add(computer.relativePosition(possibleNewLatestStamp, oldResultStamp));
                return true;
            });
            if (relativePositions.size() == 1) {
                switch ((RelativePosition) relativePositions.toArray()[0]) {
                    case AFTER:
                        latestResult.add(possibleNewLatestStamp);
                        break;
                    case BEFORE:
                    case UNREACHABLE:
                        oldResult.keys().forEach((oldResultStamp) -> {
                            latestResult.add(oldResultStamp);
                            return true;
                        });
                        break;
                    case CONTRADICTION:
                    case EQUAL:
                        latestResult.add(possibleNewLatestStamp);
                        oldResult.keys().forEach((oldResultStamp) -> {
                            latestResult.add(oldResultStamp);
                            return true;
                        });
                        break;

                    default:
                        throw new UnsupportedOperationException("Can't handle: " + relativePositions.toArray()[0]);
                }
            } else {
                oldResult.add(possibleNewLatestStamp);
                String stampInfo = Stamp.stampArrayToString(oldResult.keys().elements());
                System.out.println(stampInfo);
                throw new UnsupportedOperationException("Can't compute latest stamp for: " + stampInfo);
            }
        }
    }

}
