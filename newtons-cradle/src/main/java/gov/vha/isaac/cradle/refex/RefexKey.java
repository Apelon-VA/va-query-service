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
package gov.vha.isaac.cradle.refex;

/**
 *
 * @author kec
 */
public class RefexKey
    implements Comparable<RefexKey> {
    int key1;
    int refexSequence;

    public RefexKey(int key1, int refexSequence) {
        this.key1 = key1;
        this.refexSequence = refexSequence;
    }

    @Override
    public int compareTo(RefexKey o) {
        if (key1 != o.key1) {
            if (key1 < o.key1) {
                return -1;
            }
            return 1;
        }
        if (refexSequence == o.refexSequence) {
            return 0;
        }
        if (refexSequence < o.refexSequence) {
            return -1;
        }
        return 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RefexKey sememeKey = (RefexKey) o;

        if (key1 != sememeKey.key1) return false;
        return refexSequence == sememeKey.refexSequence;
    }

    @Override
    public int hashCode() {
        int result = key1;
        result = 31 * result + refexSequence;
        return result;
    }

    public int getKey1() {
        return key1;
    }

    public int getRefexSequence() {
        return refexSequence;
    }

    @Override
    public String toString() {
        return "RefexKey{" +
                "key1=" + key1 +
                 ", refexSequence=" + refexSequence +
                '}';
    }
}
