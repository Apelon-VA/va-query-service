package gov.vha.isaac.cradle.memory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Created by kec on 4/10/15.
 */
public class HoldInMemoryCache {
    private static final int CACHE_SIZE = 1024;
    private static final AtomicReferenceArray<MemoryManagedReference> cache = new AtomicReferenceArray<MemoryManagedReference>(CACHE_SIZE);
    private static final AtomicInteger cacheIndex = new AtomicInteger(0);
    private static MemoryManagedReference lastAddition = null;

    public static void addToCache(MemoryManagedReference newRef) {
        if (newRef != lastAddition) {
            lastAddition =  newRef;
            int index = cacheIndex.getAndIncrement();
            while (index >= CACHE_SIZE) {
                cacheIndex.compareAndSet(index + 1, 0);
                index = cacheIndex.getAndIncrement();
            }
            newRef.cacheEntry();
            MemoryManagedReference oldRef = cache.getAndSet(index, newRef);
            if (oldRef != null) {
                oldRef.cacheExit();
            }
        }
    }
}
