/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.collections;

import gov.vha.isaac.cradle.CradleObject;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 *
 * @author kec
 * @param <T>
 */
public class CasSequenceObjectMap<T extends CradleObject> {

    private static final int SEGMENT_SIZE = 1280;

    CradleMergeSerializer<T> serializer;
    CopyOnWriteArrayList<SerializedAtomicReferenceArray> objectByteList = new CopyOnWriteArrayList<>();

    CopyOnWriteArrayList<Boolean> changed = new CopyOnWriteArrayList<>();

    public CasSequenceObjectMap(CradleMergeSerializer<T> serializer) {
        this.serializer = serializer;
        objectByteList.add(new SerializedAtomicReferenceArray(SEGMENT_SIZE, serializer, objectByteList.size()));
        changed.add(Boolean.FALSE);
    }

    public void read(Path dbFolderPath, String folder, String suffix) {
        objectByteList.clear();
        
        int segment = 0;
        int segments = 1;

        while (segment < segments) {
            File segmentFile = new File(dbFolderPath.toFile(), folder + segment + suffix);
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
    private T getQuick(int sequence) {
        int segmentIndex = sequence / SEGMENT_SIZE;
        int indexInSegment = sequence % SEGMENT_SIZE;

        byte[] objectBytes = objectByteList.get(segmentIndex).get(indexInSegment);
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(objectBytes))) {
            return serializer.deserialize(dis, CradleObject.digest(objectBytes));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
            try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(objectBytes))) {
                return Optional.of(serializer.deserialize(dis, CradleObject.digest(objectBytes)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return Optional.empty();
    }
    ReentrantLock expandLock = new ReentrantLock();

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

        try {
            byte[] newBytes;
            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(128)) {
                serializer.serialize(new DataOutputStream(byteArrayOutputStream), value);
                newBytes = byteArrayOutputStream.toByteArray();
            }

            byte[] currentBytes = objectByteList.get(segmentIndex).get(indexInSegment);

            long[] md5Data = null;
            if (currentBytes != null) {
                if (Arrays.equals(newBytes, currentBytes)) {
                    return true;
                }
                md5Data = CradleObject.digest(currentBytes);
            }
            while (true) {
                while (!value.verifyDigest(currentBytes)) {
                    try (ByteArrayInputStream bais = new ByteArrayInputStream(currentBytes)) {
                        T currentWrittenObject = serializer.deserialize(new DataInputStream(bais), md5Data);
                        value = serializer.merge(value, currentWrittenObject, md5Data);
                        currentBytes = objectByteList.get(segmentIndex).get(indexInSegment);
                        md5Data = CradleObject.digest(currentBytes);
                    }
                }
                try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(128)) {
                    serializer.serialize(new DataOutputStream(byteArrayOutputStream), value);
                    newBytes = byteArrayOutputStream.toByteArray();
                    if (newBytes.length > 0) {
                        if (objectByteList.get(segmentIndex).compareAndSet(indexInSegment, currentBytes, newBytes)) {
                            changed.set(segmentIndex, Boolean.TRUE);
                            return true;
                        }
                        currentBytes = objectByteList.get(segmentIndex).get(indexInSegment);
                        md5Data = CradleObject.digest(currentBytes);
                    } else {
                        return false; // no write for null value...
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
