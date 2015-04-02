/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.waitfree;

import gov.vha.isaac.ochre.model.WaitFreeComparable;
import gov.vha.isaac.cradle.collections.SerializedAtomicReferenceArray;
import gov.vha.isaac.ochre.model.DataBuffer;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 *
 * @author kec
 * @param <T>
 */
public class CasSequenceObjectMap<T extends WaitFreeComparable> {

    private static final int SEGMENT_SIZE = 1280;
    private static final int WRITE_SEQUENCES = 64;

    private static final AtomicIntegerArray writeSequences = new AtomicIntegerArray(WRITE_SEQUENCES);

    private static int getWriteSequence(int componentSequence) {
        return writeSequences.incrementAndGet(componentSequence % WRITE_SEQUENCES);
    }

    ReentrantLock expandLock = new ReentrantLock();

    WaitFreeMergeSerializer<T> serializer;
    CopyOnWriteArrayList<SerializedAtomicReferenceArray> objectByteList = new CopyOnWriteArrayList<>();

    CopyOnWriteArrayList<Boolean> changed = new CopyOnWriteArrayList<>();

    public CasSequenceObjectMap(WaitFreeMergeSerializer<T> serializer) {
        this.serializer = serializer;
        objectByteList.add(new SerializedAtomicReferenceArray(SEGMENT_SIZE, serializer, objectByteList.size()));
        changed.add(Boolean.FALSE);
    }

    /**
     * Read from disk
     *
     * @param dbFolderPath folder where the data is located
     * @param filePrefix prefix for files that contain segment data
     * @param fileSuffix suffix for files that contain segment data
     */
    public void read(Path dbFolderPath, String filePrefix, String fileSuffix) {
        objectByteList.clear();

        int segment = 0;
        int segments = 1;

        while (segment < segments) {
            File segmentFile = new File(dbFolderPath.toFile(), filePrefix + segment + fileSuffix);
            try (DataInputStream input = new DataInputStream(new BufferedInputStream(new FileInputStream(segmentFile)))) {
                segments = input.readInt();
                int segmentIndex = input.readInt();
                int segmentArrayLength = input.readInt();

                SerializedAtomicReferenceArray referenceArray = new SerializedAtomicReferenceArray(SEGMENT_SIZE, serializer, objectByteList.size());
                objectByteList.add(referenceArray);

                for (int i = 0; i < segmentArrayLength; i++) {
                    int byteArrayLength = input.readInt();
                    if (byteArrayLength > 0) {
                        byte[] bytes = new byte[byteArrayLength];
                        input.read(bytes);
                        referenceArray.set(i, bytes);
                    }
                }

            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            segment++;
        }

    }

    public void write(Path dbFolderPath, String folder, String suffix) {
        int segments = objectByteList.size();
        for (int segmentIndex = 0; segmentIndex < segments; segmentIndex++) {
            File segmentFile = new File(dbFolderPath.toFile(), folder + segmentIndex + suffix);
            segmentFile.getParentFile().mkdirs();
            SerializedAtomicReferenceArray segmentArray = objectByteList.get(segmentIndex);
            try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(segmentFile)))) {
                output.writeInt(segments);
                output.writeInt(segmentIndex);
                output.writeInt(segmentArray.length());
                for (int indexValue = 0; indexValue < SEGMENT_SIZE; indexValue++) {
                    byte[] value = segmentArray.get(indexValue);
                    if (value == null) {
                        output.writeInt(-1);
                    } else {
                        output.writeInt(value.length);
                        output.write(value);
                    }
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public Stream<T> getStream() {
        IntStream sequences = IntStream.range(0, objectByteList.size() * SEGMENT_SIZE);
        return sequences.filter(sequence -> containsKey(sequence)).mapToObj(sequence -> getQuick(sequence));
    }

    public Stream<T> getParallelStream() {
        IntStream sequences = IntStream.range(0, objectByteList.size() * SEGMENT_SIZE).parallel();
        return sequences.filter(sequence -> containsKey(sequence)).mapToObj(sequence -> getQuick(sequence));
    }

    /**
     * Provides no range or null checking. For use with a stream that already
     * filters out null values and out of range sequences.
     *
     * @param sequence
     * @return
     */
    public T getQuick(int sequence) {
        int segmentIndex = sequence / SEGMENT_SIZE;
        int indexInSegment = sequence % SEGMENT_SIZE;

        DataBuffer buff = new DataBuffer(objectByteList.get(segmentIndex).get(indexInSegment));
        return serializer.deserialize(buff);
    }

    public int getSize() {
        return (int) getParallelStream().count();
    }

    public boolean containsKey(int sequence) {
        int segmentIndex = sequence / SEGMENT_SIZE;
        int indexInSegment = sequence % SEGMENT_SIZE;
        if (segmentIndex >= objectByteList.size()) {
            return false;
        }
        return objectByteList.get(segmentIndex).get(indexInSegment) != null;
    }

    public Optional<T> get(int sequence) {

        int segmentIndex = sequence / SEGMENT_SIZE;
        if (segmentIndex >= objectByteList.size()) {
            return Optional.empty();
        }
        int indexInSegment = sequence % SEGMENT_SIZE;

        byte[] objectBytes = objectByteList.get(segmentIndex).get(indexInSegment);
        if (objectBytes != null) {
            DataBuffer buf = new DataBuffer(objectBytes);
            return Optional.of(serializer.deserialize(buf));
        }
        return Optional.empty();
    }

    public boolean put(int sequence, T value) {

        int segmentIndex = sequence / SEGMENT_SIZE;

        if (segmentIndex >= objectByteList.size()) {
            expandLock.lock();
            try {
                while (segmentIndex >= objectByteList.size()) {
                    changed.add(Boolean.FALSE);
                    objectByteList.add(new SerializedAtomicReferenceArray(SEGMENT_SIZE, serializer, objectByteList.size()));
                }
            } finally {
                expandLock.unlock();
            }
        }
        int indexInSegment = sequence % SEGMENT_SIZE;
        //
        int oldWriteSequence = value.getWriteSequence();
        int oldDataSize = 0;
        byte[] oldData = objectByteList.get(segmentIndex).get(indexInSegment);
        if (oldData != null) {
            oldWriteSequence = getWriteSequence(oldData);
            oldDataSize = oldData.length;
        }
        
        while (true) {
            if (oldWriteSequence != value.getWriteSequence()) {
                // need to merge.
                DataBuffer oldDataBuffer = new DataBuffer(oldData);
                T oldObject = serializer.deserialize(oldDataBuffer);
                value = serializer.merge(value, oldObject, oldWriteSequence);
            }
            value.setWriteSequence(getWriteSequence(sequence));
            
            DataBuffer newDataBuffer = new DataBuffer(oldDataSize + 512);
            serializer.serialize(newDataBuffer, value);
            newDataBuffer.trimToSize();
            if (objectByteList.get(segmentIndex).compareAndSet(indexInSegment, oldData, newDataBuffer.getData())) {
                changed.set(segmentIndex, Boolean.TRUE);
                return true;
            }

            // Try again.
            oldData = objectByteList.get(segmentIndex).get(indexInSegment);
            oldWriteSequence = getWriteSequence(oldData);
        }

    }

    public int getWriteSequence(byte[] data) {
        return (((data[0]) << 24)
                | ((data[1] & 0xff) << 16)
                | ((data[2] & 0xff) << 8)
                | ((data[3] & 0xff)));
    }
}
