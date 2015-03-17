package gov.vha.isaac.cradle;

import gov.vha.isaac.ochre.collections.NidSet;
import gov.vha.isaac.ochre.collections.SequenceSet;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;
import org.ihtsdo.otf.tcc.api.nid.ConcurrentBitSet;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;

/**
 * Created by kec on 12/18/14.
 */
public class ConcurrentSequenceIntMap {

    private static final int SEGMENT_SIZE = 128000;
    ReentrantLock lock = new ReentrantLock();
    
    int[][] sequenceIntList = new int[1][];
    boolean[] changed = new boolean[1];
    AtomicInteger size = new AtomicInteger(0);
    
    public ConcurrentSequenceIntMap() {
        sequenceIntList[0] = new int[SEGMENT_SIZE];
        changed[0] = false;
    }
    
    public void read(File folder) throws IOException {
        sequenceIntList = null;
        int segments = 1;
        for (int segment = 0; segment < segments; segment++) {
            try (DataInputStream in = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(new File(folder, segment + ".sequence-int.map"))))) {                
                int segmentSize = in.readInt();
                segments = in.readInt();
                if (sequenceIntList == null) {
                    sequenceIntList = new int[segments][];
                    changed = new boolean[segments];
                }
                sequenceIntList[segment] = new int[segmentSize];
                changed[segment] = false;
                
                for (int indexInSegment = 0; indexInSegment < segmentSize; indexInSegment++) {
                    sequenceIntList[segment][indexInSegment] = in.readInt();
                }
                size.set(in.readInt());
            }
        }
    }
    
    public void write(File folder) throws IOException {
        folder.mkdirs();
        int segments = sequenceIntList.length;
        for (int segment = 0; segment < segments; segment++) {
            try (DataOutputStream out = new DataOutputStream(
                    new BufferedOutputStream(new FileOutputStream(new File(folder, segment + ".sequence-int.map"))))) {                
                out.writeInt(SEGMENT_SIZE);
                out.writeInt(segments);
                for (int indexInSegment = 0; indexInSegment < SEGMENT_SIZE; indexInSegment++) {
                    out.writeInt(sequenceIntList[segment][indexInSegment]);
                }
                out.writeInt(size.get());
            }
        }
    }
    
    public int getSize() {
        return size.get();
    }
    
    public boolean containsKey(int sequence) {
        if (sequence < 0) {
            sequence = sequence - Integer.MIN_VALUE;
        }   
        int segmentIndex = sequence / SEGMENT_SIZE;
        int indexInSegment = sequence % SEGMENT_SIZE;
        if (segmentIndex >= sequenceIntList.length) {
            return false;
        }
        return sequenceIntList[segmentIndex][indexInSegment] != 0;
    }
    
    public OptionalInt get(int sequence) {
        if (sequence < 0) {
            sequence = sequence - Integer.MIN_VALUE;
        }        
        int segmentIndex = sequence / SEGMENT_SIZE;
        if (segmentIndex >= sequenceIntList.length) {
            return OptionalInt.empty();
        }
        
        int indexInSegment = sequence % SEGMENT_SIZE;
        int returnValue = sequenceIntList[segmentIndex][indexInSegment];
        if (returnValue == 0) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(returnValue);
    }
    
    public boolean put(int sequence, int value) {
        if (sequence < 0) {
            sequence = sequence - Integer.MIN_VALUE;
        }
        size.set(Math.max(sequence, size.get()));
        int segmentIndex = sequence / SEGMENT_SIZE;
        
        if (segmentIndex >= sequenceIntList.length) {
            lock.lock();
            try {
                while (segmentIndex >= sequenceIntList.length) {
                    changed = Arrays.copyOf(changed, sequenceIntList.length + 1);
                    changed[sequenceIntList.length] = false;
                    int[][] tempList = Arrays.copyOf(sequenceIntList, sequenceIntList.length + 1);
                    tempList[tempList.length - 1] = new int[SEGMENT_SIZE];
                    sequenceIntList = tempList;
                }
            } finally {
                lock.unlock();
            }
        }
        int indexInSegment = sequence % SEGMENT_SIZE;
        sequenceIntList[segmentIndex][indexInSegment] = value;
        changed[segmentIndex] = true;
        return true;
    }
    
    public NativeIdSetBI getKeysForValues(NativeIdSetBI values) {
        int componentSize = size.get();
        ConcurrentBitSet componentNids = new ConcurrentBitSet(size.get());
        for (int i = 0; i < componentSize; i++) {
            int segmentIndex = i / SEGMENT_SIZE;
            int indexInSegment = i % SEGMENT_SIZE;
            if (sequenceIntList[segmentIndex][indexInSegment] != 0) {
                if (values.contains(sequenceIntList[segmentIndex][indexInSegment])) {
                    componentNids.set(i + Integer.MIN_VALUE);
                }
            }
        }
        return componentNids;
    }

    public NativeIdSetBI getComponentNids() {
        int componentSize = size.get();
        ConcurrentBitSet componentNids = new ConcurrentBitSet(size.get());
        for (int i = 0; i < componentSize; i++) {
            int segmentIndex = i / SEGMENT_SIZE;
            int indexInSegment = i % SEGMENT_SIZE;
            if (sequenceIntList[segmentIndex][indexInSegment] != 0) {
                componentNids.set(i + Integer.MIN_VALUE);
            }
        }
        return componentNids;
    }

    IntStream getComponentsNotSet() {
        IntStream.Builder builder = IntStream.builder();
        int componentSize = size.get();
        componentSize = componentSize - SEGMENT_SIZE;
        for (int i = 0; i < componentSize; i++) {
            int segmentIndex = i / SEGMENT_SIZE;
            int indexInSegment = i % SEGMENT_SIZE;
            if (sequenceIntList[segmentIndex][indexInSegment] == 0) {
                builder.add(i);
            }
        }
        return builder.build();
    }
}
