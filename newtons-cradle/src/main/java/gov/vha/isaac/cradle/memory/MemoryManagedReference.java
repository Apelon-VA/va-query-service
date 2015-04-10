package gov.vha.isaac.cradle.memory;

import gov.vha.isaac.cradle.collections.CradleSerializer;
import org.apache.mahout.math.map.HashFunctions;

import java.io.*;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * Created by kec on 4/10/15.
 */
public class MemoryManagedReference<T extends Object> extends SoftReference<T> implements
        Comparable<MemoryManagedReference> {

    private static final AtomicInteger objectIdSupplier = new AtomicInteger();

    private final int objectId = objectIdSupplier.getAndIncrement();

    private final AtomicReference<Instant> lastWriteToDisk = new AtomicReference<>(Instant.now());

    private final AtomicReference<Instant>  lastElementUpdate = new AtomicReference<>(Instant.now());
    private final AtomicReference<Instant>  lastElementRead = new AtomicReference<>(Instant.now());

    private final AtomicReference<T> strongReferenceForUpdate = new AtomicReference<>();
    private final AtomicReference<T> strongReferenceForCache= new AtomicReference<>();

    private final LongAdder hits = new LongAdder();
    private final AtomicInteger cacheCount = new AtomicInteger();

    private final File diskLocation;
    private final CradleSerializer<T> serializer;

    public MemoryManagedReference(T referent, File diskLocation, CradleSerializer<T> serializer) {
        super(referent);
        this.diskLocation = diskLocation;
        this.serializer = serializer;
    }

    public MemoryManagedReference(T referent, ReferenceQueue<? super T> q, File diskLocation,
                                  CradleSerializer<T> serializer) {
        super(referent, q);
        this.diskLocation = diskLocation;
        this.serializer = serializer;
    }

    public void elementUpdated() {
        if (this.strongReferenceForUpdate.compareAndSet(null, this.get())) {
            lastElementUpdate.set(Instant.now());
        }
    }

    public void elementRead() {
        hits.increment();
        this.lastElementRead.set(Instant.now());
    }

    public void cacheEntry() {
        int count = cacheCount.incrementAndGet();
        if (count == 1) {
            strongReferenceForCache.set(this.get());
        }
    }

    public void cacheExit() {
        int count = cacheCount.decrementAndGet();
        if (count == 0) {
            strongReferenceForCache.set(null);
        }
    }

    public void write() {
        T objectToWrite = this.get();
        if (objectToWrite != null && strongReferenceForUpdate.compareAndSet(objectToWrite, null)) {
            DiskSemaphore.acquire();
            lastWriteToDisk.set(Instant.now());
            try(DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
                    new FileOutputStream(diskLocation)))) {
                serializer.serialize(out, objectToWrite);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                DiskSemaphore.release();
            }
        }
    }

    public Duration timeSinceLastUnwrittenUpdate() {
        Instant lastUpdateInstant = lastElementUpdate.get();
        Instant lastWriteInstant = lastWriteToDisk.get();

        if (lastUpdateInstant.isAfter(lastWriteInstant)) {
            return Duration.between(lastUpdateInstant, Instant.now());
        }
        return Duration.ZERO;
    }

    @Override
    public int compareTo(MemoryManagedReference o) {
        return this.objectId - o.objectId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MemoryManagedReference<?> that = (MemoryManagedReference<?>) o;

        return objectId == that.objectId;

    }

    @Override
    public int hashCode() {
        return HashFunctions.hash(objectId);
    }
}
