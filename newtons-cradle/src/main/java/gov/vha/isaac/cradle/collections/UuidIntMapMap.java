package gov.vha.isaac.cradle.collections;

import gov.vha.isaac.cradle.DiskSemaphore;
import gov.vha.isaac.ochre.collections.uuidnidmap.ConcurrentUuidToIntHashMap;
import gov.vha.isaac.ochre.collections.uuidnidmap.UuidToIntMap;
import gov.vha.isaac.ochre.collections.uuidnidmap.UuidUtil;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by kec on 7/27/14.
 */
public class UuidIntMapMap implements UuidToIntMap {
    private static boolean DISABLE_SOFT_REFERENCES = true;

    private static final Logger log = LogManager.getLogger();
    private static final int DEFAULT_TOTAL_MAP_SIZE = 15000000;
    public static final int NUMBER_OF_MAPS = 256;
    
    private static final int DEFAULT_MAP_SIZE = DEFAULT_TOTAL_MAP_SIZE / NUMBER_OF_MAPS;
    private static final double minLoadFactor = 0.75;
    private static final double maxLoadFactor = 0.9;
    
    private static final AtomicInteger nextNidProvider = new AtomicInteger(Integer.MIN_VALUE);

    public static AtomicInteger getNextNidProvider() {
        return nextNidProvider;
    }

    public boolean shutdown = false;

    private class MapSoftReference extends SoftReference<ConcurrentUuidToIntHashMap> {
        int mapSequence;
        AtomicBoolean modified = new AtomicBoolean(false);
        ConcurrentUuidToIntHashMap theMap;

        public MapSoftReference(int mapSequence, ConcurrentUuidToIntHashMap referent) {
            super(referent, mapGcQueue);
            this.mapSequence = mapSequence;
            if (DISABLE_SOFT_REFERENCES) {
                theMap = referent;
            }
        }
    }
    
    private class MapWriter implements Runnable {

        @Override
        public void run() {
            while (!shutdown) {
                try {
                    MapSoftReference reference = (MapSoftReference) mapGcQueue.remove();
                    boolean present = reference.get() != null;
                    log.info("UuidIntMapMap diet for " + reference.mapSequence +
                        ". Referent is present: " + present);
                    if (reference.modified.get()) {
                        DiskSemaphore.acquire();
                        log.info("UuidIntMapMap DiskSemaphore.acquire() for " + reference.mapSequence);
                        try(DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
                                new FileOutputStream(new File(folder, reference.mapSequence + "-uuid-nid.map"))))) {
                            serializer.serialize(out, reference.get());
                        } finally {
                            DiskSemaphore.release();
                            log.info("UuidIntMapMap DiskSemaphore.release() for " + reference.mapSequence);
                        }
                        reference.modified.set(false);
                    } else {
                        log.info("UuidIntMapMap unmodified: " + reference.mapSequence);
                    }
                } catch (Throwable ex) {
                    log.error("MapWriter error: ", ex);
                }
            }
        }
    }

    private static final ConcurrentUuidIntMapSerializer serializer = new ConcurrentUuidIntMapSerializer();

    MapSoftReference[] maps = new MapSoftReference[NUMBER_OF_MAPS];
    ReferenceQueue<ConcurrentUuidToIntHashMap> mapGcQueue = new ReferenceQueue();
    File folder;
    Thread writerThread;

    private UuidIntMapMap(File folder) {
        this.folder = folder;
        for (int i = 0; i < maps.length; i++) {
            maps[i] = new MapSoftReference(i, new ConcurrentUuidToIntHashMap(DEFAULT_MAP_SIZE, minLoadFactor, maxLoadFactor));
        }
        writerThread = new Thread(new MapWriter(), "UuidIntMapMap writer");
        writerThread.setDaemon(true);
    }
    
    public static UuidIntMapMap create(File folder) {
        UuidIntMapMap returnValue = new UuidIntMapMap(folder);
        returnValue.writerThread.start();
        return returnValue;
    }
    
    public void write() throws IOException {
        folder.mkdirs();
        for (int i = 0; i < NUMBER_OF_MAPS; i++) {
            ConcurrentUuidToIntHashMap map = maps[i].get();
            if (map != null && maps[i].modified.get()) {
                DiskSemaphore.acquire();
                try(DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
                    new FileOutputStream(new File(folder, i + "-uuid-nid.map"))))) {
                    serializer.serialize(out, map);
                    maps[i].modified.set(false);
                } finally {
                    DiskSemaphore.release();
                }
            }
        }
    }
    
    public void read() throws IOException {
        log.info("Starting UuidIntMapMap load. ");
        for (int i = 0; i < NUMBER_OF_MAPS; i++) {
            readMapFromDisk(i);
        }
        log.info("Finished UuidIntMapMap load. ");
    }

    protected void readMapFromDisk(int i) throws IOException {
        DiskSemaphore.acquire();
        try(DataInputStream in = new DataInputStream(new BufferedInputStream(
                new FileInputStream(new File(folder, i + "-uuid-nid.map"))))) {
            maps[i] = new MapSoftReference(i, serializer.deserialize(in));
            log.debug("UuidIntMapMap restored: " + i);
        } finally {
            DiskSemaphore.release();
        }
    }

    private ConcurrentUuidToIntHashMap getMap(UUID key) {
        if (key == null) {
            throw new IllegalStateException("UUIDs cannot be null. ");
        }
        int index = getMapIndex(key);
        return getMap(index);
    }

    protected ConcurrentUuidToIntHashMap getMap(int index) throws RuntimeException {
        ConcurrentUuidToIntHashMap result = maps[index].get();
        while (result == null) {
            try {
                readMapFromDisk(index);
                result = maps[index].get();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        return result;
    }

    @Override
    public boolean containsKey(UUID key) {
        return getMap(key).containsKey(key);
    }

    @Override
    public boolean containsValue(int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int get(UUID key) {
        return getMap(key).get(key);
    }

    public int getWithGeneration(UUID uuidKey) {
         
        long[] keyAsArray = UuidUtil.convert(uuidKey);
    
        int mapIndex = getMapIndex(uuidKey);
        int nid = getMap(mapIndex).get(keyAsArray);
        if (nid != Integer.MAX_VALUE) {
            return nid;
        }
        ConcurrentUuidToIntHashMap map = getMap(mapIndex);
        long stamp = map.getStampedLock().writeLock();
        try {
            nid = map.get(keyAsArray, stamp);
            if (nid != Integer.MAX_VALUE) {
                return nid;
            }
            nid = nextNidProvider.incrementAndGet();
//            if (nid == -2147483637) {
//                System.out.println(nid + "->" + key);
//            }
            map.put(keyAsArray, nid, stamp);
            maps[mapIndex].modified.set(true);
            return nid;
        } finally {
            map.getStampedLock().unlockWrite(stamp);
        }
    }

    @Override
    public boolean put(UUID uuidKey, int value) {
        int mapIndex = getMapIndex(uuidKey);
        long[] keyAsArray = UuidUtil.convert(uuidKey);
        ConcurrentUuidToIntHashMap map = getMap(mapIndex);
        long stamp = map.getStampedLock().writeLock();
        try {
            boolean returnValue = map.put(keyAsArray, value, stamp);
            maps[mapIndex].modified.set(true);
            return returnValue;
        } finally {
            map.getStampedLock().unlockWrite(stamp);
        }
    }

    public int size() {
        int size = 0;
        for (int i = 0; i < maps.length; i++) {
            size += getMap(i).size();
        }
        return size;
    }

    private int getMapIndex(UUID key) {
        return ((int) ((byte) key.hashCode())) - Byte.MIN_VALUE;
    }


    public UUID[] getKeysForValue(int value) {
        ArrayList<UUID> uuids = new ArrayList<>();
        for (int index = 0; index < maps.length; index++) {
            getMap(index).keysOf(value).stream().forEach(uuid -> {
                uuids.add(uuid);
            });
        }
        return uuids.toArray(new UUID[uuids.size()]);
    }
    
    public void reportStats(Logger log) {
        for (int i = 0; i < NUMBER_OF_MAPS; i++) {
            log.info("UUID map: " + i + " " + getMap(i).getStats());
        }
    }


    public boolean isShutdown() {
        return shutdown;
    }

    public void setShutdown(boolean shutdown) {
        this.shutdown = shutdown;
    }
}
