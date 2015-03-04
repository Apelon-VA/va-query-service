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

import java.util.function.BiConsumer;
import java.util.stream.IntStream;
import org.apache.mahout.math.set.OpenIntHashSet;
import org.ihtsdo.otf.tcc.model.version.RelativePosition;

/**
 *
 * @author kec
 */
public class LatestPrimitiveStampCombiner implements BiConsumer<LatestStampsAsIntArray, LatestStampsAsIntArray> {

    private final StampSequenceComputer computer;

    public LatestPrimitiveStampCombiner(StampSequenceComputer computer) {
        this.computer = computer;
    }

    @Override
    public void accept(LatestStampsAsIntArray t, LatestStampsAsIntArray u) {
        OpenIntHashSet tStampSet = t.getLatestStamps();
        OpenIntHashSet uStampSet = u.getLatestStamps();
        t.reset();
        if (tStampSet.size() == 1 && uStampSet.size() == 1) {

            int stamp1 = tStampSet.keys().get(0);
            int stamp2 = uStampSet.keys().get(0);
            RelativePosition relativePosition = computer.relativePosition(stamp1, stamp2);
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
                    if (computer.onRoute(stamp2)) {
                        t.add(stamp1);
                        t.add(stamp2);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Can't handle: " + relativePosition);

            }

        } else {
            uStampSet.forEachKey((stamp) -> {
                tStampSet.add(stamp);
                return true;
            });
            t.reset();
            t.addAll(computer.getLatestStamps(IntStream.of(tStampSet.keys().elements())));
        }
    }

}
