/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.collections;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.OptionalInt;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;

/**
 *
 * @author kec
 */
public class SequenceMap {
   
    private static final double minimumLoadFactor = 0.75;
    private static final double maximumLoadFactor = 0.9;

    ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();

    int nextSequence = 0;

 
    final NativeIntIntHashMap nidSequenceMap;
    final NativeIntIntHashMap sequenceNidMap;

    public SequenceMap(int defaultCapacity) {
        nidSequenceMap = new NativeIntIntHashMap(defaultCapacity, minimumLoadFactor, maximumLoadFactor);
        sequenceNidMap = new NativeIntIntHashMap(defaultCapacity, minimumLoadFactor, maximumLoadFactor);
    }

    
    
    public int getNextSequence() {
        return nextSequence;
    }

   public int getSize() {
        assert nidSequenceMap.size() == sequenceNidMap.size() : "nidSequenceMap.size() = "
                + nidSequenceMap.size() + " sequenceNidMap.size() = " + sequenceNidMap.size();
        return sequenceNidMap.size();
    }

    public boolean containsNid(int nid) {
        rwl.readLock().lock();
        try {
            return nidSequenceMap.containsKey(nid);
        } finally {
            rwl.readLock().unlock();
        }
    }
    public int getSequenceFast(int nid) {

        rwl.readLock().lock();
        try {
           return nidSequenceMap.get(nid);
        } finally {
            rwl.readLock().unlock();
        }
    }

    public OptionalInt getSequence(int nid) {

        rwl.readLock().lock();
        try {
            if (nidSequenceMap.containsKey(nid)) {
                return OptionalInt.of(nidSequenceMap.get(nid));
            }
            return OptionalInt.empty();
        } finally {
            rwl.readLock().unlock();
        }
    }

    public OptionalInt getNid(int sequence) {
        rwl.readLock().lock();
        try {
            if (sequenceNidMap.containsKey(sequence)) {
                return OptionalInt.of(sequenceNidMap.get(sequence));
            }
            return OptionalInt.empty();
        } finally {
            rwl.readLock().unlock();
        }
    }

    
    public int getNidFast(int sequence) {
        return sequenceNidMap.get(sequence);
    }
    
    public int addNid(int nid) {
        rwl.writeLock().lock();
        try {
            if (!nidSequenceMap.containsKey(nid)) {
                int sequence = nextSequence++;
                nidSequenceMap.put(nid, sequence);
                sequenceNidMap.put(sequence, nid);
                return sequence;
            }
            return nidSequenceMap.get(nid);
        } finally {
            rwl.writeLock().unlock();
        }
    }

    public int addNidIfMissing(int nid) {

        rwl.readLock().lock();
        try {
            if (nidSequenceMap.containsKey(nid)) {
                return nidSequenceMap.get(nid);
            }
        } finally {
            rwl.readLock().unlock();
        }
        rwl.writeLock().lock();
        try {
            if (!nidSequenceMap.containsKey(nid)) {
                int sequence = nextSequence++;
                nidSequenceMap.put(nid, sequence);
                sequenceNidMap.put(sequence, nid);
                return sequence;
            }
            return nidSequenceMap.get(nid);
        } finally {
            rwl.writeLock().unlock();
        }
    }

    public IntStream getSequenceStream() {
        return IntStream.of(sequenceNidMap.keys().elements());
    }

    public IntStream getConceptNidStream() {
        return IntStream.of(nidSequenceMap.keys().elements());
    }

    public void write(File mapFile) throws IOException {
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(mapFile)))) {
            output.writeInt(nidSequenceMap.size());
            output.writeInt(nextSequence);
            nidSequenceMap.forEachPair((int nid, int sequence) -> {
                try {
                    output.writeInt(nid);
                    output.writeInt(sequence);
                    return true;
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            });
        }
    }

    public void read(File mapFile) throws IOException {
        try (DataInputStream input = new DataInputStream(new BufferedInputStream(new FileInputStream(mapFile)))) {
            int size = input.readInt();
            nextSequence = input.readInt();
            nidSequenceMap.ensureCapacity(size);
            sequenceNidMap.ensureCapacity(size);
            for (int i = 0; i < size; i++) {
                int nid = input.readInt();
                int sequence = input.readInt();
                nidSequenceMap.put(nid, sequence);
                sequenceNidMap.put(sequence, nid);
            }
        }
    }

}
