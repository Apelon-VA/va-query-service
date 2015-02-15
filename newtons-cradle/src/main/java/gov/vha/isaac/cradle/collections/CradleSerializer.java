/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.collections;

import java.io.DataInput;
import java.io.DataOutput;

/**
 *
 * @author kec
 */
public interface CradleSerializer<T> {

    /**
     * Serialize the content of an object into a ObjectOutput
     *
     * @param out ObjectOutput to save object into
     * @param value Object to serialize
     */
    public void serialize( DataOutput out, T value);


    /**
     * Deserialize the content of an object from a DataInput.
     *
     * @param in to read serialized data from
     * @return deserialized object
     */
    public T deserialize(DataInput in);

}