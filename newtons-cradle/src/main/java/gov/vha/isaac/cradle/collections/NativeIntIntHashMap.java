/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.collections;

import org.apache.mahout.math.map.OpenIntIntHashMap;

/**
 *
 * @author kec
 */
public class NativeIntIntHashMap extends OpenIntIntHashMap {

    public NativeIntIntHashMap() {
    }

    public NativeIntIntHashMap(int initialCapacity) {
        super(initialCapacity);
    }

    public NativeIntIntHashMap(int initialCapacity, double minLoadFactor, double maxLoadFactor) {
        super(initialCapacity, minLoadFactor, maxLoadFactor);
    }

    public int[] getTable() {
        return table;
    }

    public void setTable(int[] table) {
        this.table = table;
    }

    public int[] getValues() {
        return values;
    }

    public void setValues(int[] values) {
        this.values = values;
    }

    public byte[] getState() {
        return state;
    }

    public void setState(byte[] state) {
        this.state = state;
    }

    public int getFreeEntries() {
        return freeEntries;
    }

    public void setFreeEntries(int freeEntries) {
        this.freeEntries = freeEntries;
    }

    public int getDistinct() {
        return distinct;
    }

    public void setDistinct(int distinct) {
        this.distinct = distinct;
    }
}
