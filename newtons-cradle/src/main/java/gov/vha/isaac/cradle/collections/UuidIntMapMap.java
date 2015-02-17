package gov.vha.isaac.cradle.collections;

import gov.vha.isaac.ochre.collections.uuidnidmap.ConcurrentUuidToIntHashMap;
import gov.vha.isaac.ochre.collections.uuidnidmap.UuidArrayList;
import gov.vha.isaac.ochre.collections.uuidnidmap.UuidToIntMap;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by kec on 7/27/14.
 */
public class UuidIntMapMap implements UuidToIntMap {
    private static final Logger log = LogManager.getLogger();
    private static final int DEFAULT_MAP_SIZE = 155027;
    public static final int NUMBER_OF_MAPS = 256;


    ConcurrentUuidToIntHashMap[] maps = new ConcurrentUuidToIntHashMap[NUMBER_OF_MAPS];
    int[] nextIdArray = new int[NUMBER_OF_MAPS];

    {
        for (int i = 0; i < NUMBER_OF_MAPS; i++) {
            nextIdArray[i] = Integer.MIN_VALUE + i;
        }
    }



    public UuidIntMapMap(int[] nextIdArray,
                         ConcurrentNavigableMap<Integer, ConcurrentUuidToIntHashMap> uuidNidMapMap) {
        this.nextIdArray = nextIdArray;
        assert nextIdArray.length == NUMBER_OF_MAPS;
        for (int i = 0; i < uuidNidMapMap.size(); i++) {
            maps[i] = uuidNidMapMap.get(i);
        }
        for (int i = 0; i < maps.length; i++) {
            maps[i] = uuidNidMapMap.get(i);
            nextIdArray[i] = Integer.MIN_VALUE + i;
            if (maps[i] == null) {
                maps[i] = new ConcurrentUuidToIntHashMap(DEFAULT_MAP_SIZE);
            } else {
                log.trace("Found map of size: {} for index: {}", maps[i].size(), i);
            }
        }
    }

    public UuidIntMapMap() {
        assert nextIdArray.length == NUMBER_OF_MAPS;
        for (int i = 0; i < maps.length; i++) {
            maps[i] = new ConcurrentUuidToIntHashMap();
            nextIdArray[i] = Integer.MIN_VALUE + i;
            if (maps[i] == null) {
                maps[i] = new ConcurrentUuidToIntHashMap(DEFAULT_MAP_SIZE);
            } else {
                log.trace("Found map of size: {} for index: {}", maps[i].size(), i);
            }
        }
    }
    
    public void write(File folder) throws IOException {
        folder.mkdirs();
        ConcurrentUuidIntMapSerializer serializer = new ConcurrentUuidIntMapSerializer();
        for (int i = 0; i < NUMBER_OF_MAPS; i++) {
            try(DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
                    new FileOutputStream(new File(folder, i + "-uuid-nid.map"))))) {
                serializer.serialize(out, maps[i]);
            }
        }
    }
    public void read(File folder) throws IOException {
        ConcurrentUuidIntMapSerializer serializer = new ConcurrentUuidIntMapSerializer();
        for (int i = 0; i < NUMBER_OF_MAPS; i++) {
            try(DataInputStream in = new DataInputStream(new BufferedInputStream(
                    new FileInputStream(new File(folder, i + "-uuid-nid.map"))))) {
                maps[i] = serializer.deserialize(in);
            }
        }
    }


    @Override
    public boolean containsKey(UUID key) {
        if (key == null) {
            throw new IllegalStateException("UUIDs cannot be null. ");
        }
        return maps[getMapIndex(key)].containsKey(key);
    }

    @Override
    public boolean containsValue(int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int get(UUID key) {
        return maps[getMapIndex(key)].get(key);
    }

    public int getWithGeneration(UUID key) {
        int mapIndex = getMapIndex(key);
        int nid = maps[mapIndex].get(key);
        if (nid != Integer.MAX_VALUE) {
            return nid;
        }
        maps[mapIndex].getWriteLock().lock();
        try {
            nid = maps[mapIndex].get(key);
            if (nid != Integer.MAX_VALUE) {
                return nid;
            }
            nid = nextIdArray[mapIndex];
            nextIdArray[mapIndex] = nextIdArray[mapIndex] + NUMBER_OF_MAPS;
            maps[mapIndex].put(key, nid);
            return nid;
        } finally {
            maps[mapIndex].getWriteLock().unlock();
        }
    }

    @Override
    public boolean put(UUID key, int value) {
        return maps[getMapIndex(key)].put(key, value);
    }

    public int size() {
        int size = 0;
        for (ConcurrentUuidToIntHashMap map: maps) {
            size += map.size();
        }
        return size;
    }

    private int getMapIndex(UUID key) {
        return ((int) ((byte) key.hashCode())) - Byte.MIN_VALUE;
    }
    
    public int[] getNextIdArray() {
        return nextIdArray;
    }

    public UUID[] getKeysForValue(int i) {
        ArrayList<UUID> uuids = new ArrayList<>();
        for (ConcurrentUuidToIntHashMap map: maps) {
            map.keysOf(i).stream().forEach(uuid -> {
                uuids.add(uuid);
            });
        }
        return uuids.toArray(new UUID[uuids.size()]);
    }
}
