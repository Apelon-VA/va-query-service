package gov.vha.isaac.cradle;

import org.apache.mahout.math.map.OpenObjectIntHashMap;

import java.util.OptionalInt;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by kec on 12/18/14.
 */
public class ConcurrentObjectIntMap<T> {
    ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();

    OpenObjectIntHashMap<T> backingMap = new OpenObjectIntHashMap<>();


    public boolean containsKey(T key) {
        rwl.readLock().lock();
        try {
            return backingMap.containsKey(key);
        } finally {
            rwl.readLock().unlock();
        }
    }

    public OptionalInt get(T key) {
        int value;
        rwl.readLock().lock();
        try {
            if (backingMap.containsKey(key)) {
                return OptionalInt.of(backingMap.get(key));
            }
            return OptionalInt.empty();
        } finally {
            rwl.readLock().unlock();
        }
    }

    public boolean put(T key, int value) {

        rwl.writeLock().lock();
        try {
            return backingMap.put(key, value);
        } finally {
            rwl.writeLock().unlock();
        }
    }

    public int size() {
        return backingMap.size();
    }

}
