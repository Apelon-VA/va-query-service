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
 * @param <T>
 */
public interface CradleMergeSerializer<T> {
    
    void serialize(DataOutput d, T a);

    /**
     * Support for merging objects when a compare and swap operation fails. 
     * @param a
     * @param b
     * @param md5Data
     * @return 
     */
    T merge (T a, T b, long[] md5Data);

    T deserialize(DataInput di, long[] md5Data);

}
