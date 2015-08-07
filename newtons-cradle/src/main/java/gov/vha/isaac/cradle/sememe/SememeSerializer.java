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
package gov.vha.isaac.cradle.sememe;

import gov.vha.isaac.cradle.waitfree.WaitFreeMergeSerializer;
import gov.vha.isaac.ochre.model.DataBuffer;
import gov.vha.isaac.ochre.model.sememe.SememeChronologyImpl;

/**
 *
 * @author kec
 */
public class SememeSerializer implements WaitFreeMergeSerializer<SememeChronologyImpl<?>>{

    @Override
    public void serialize(DataBuffer d, SememeChronologyImpl<?> a) {
        byte[] data = a.getDataToWrite();
        d.put(data, 0, data.length);
    }

    @Override
    public SememeChronologyImpl<?> merge(SememeChronologyImpl<?> a, SememeChronologyImpl<?> b, int writeSequence) {
        byte[] dataBytes = a.mergeData(writeSequence, b.getDataToWrite(writeSequence));
        DataBuffer db = new DataBuffer(dataBytes);
        return new SememeChronologyImpl<>(db);
    }

    @Override
    public SememeChronologyImpl<?> deserialize(DataBuffer db) {
       return new SememeChronologyImpl<>(db);
    }
    
}
