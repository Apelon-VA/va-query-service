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

import org.apache.mahout.math.set.OpenIntHashSet;

/**
 *
 * @author kec
 */
public class LatestStampsAsIntArray {

    private final OpenIntHashSet latestStamps = new OpenIntHashSet();

    public void addAll(OpenIntHashSet stamps) {
        stamps.forEachKey((stamp) -> {
            latestStamps.add(stamp);
            return true;
        });
    }
    public void addAll(int[] stamps) {
        for (int stamp: stamps) {
            latestStamps.add(stamp);
        }
    }

    public OpenIntHashSet getLatestStamps() {
        OpenIntHashSet latest = new OpenIntHashSet();
        latestStamps.forEachKey((int stamp) -> {
            latest.add(stamp);
            return true;
        });
        return latest;
    }

    public void add(int stamp) {
        latestStamps.add(stamp);
    }

    public void reset() {
        latestStamps.clear();
    }

}
