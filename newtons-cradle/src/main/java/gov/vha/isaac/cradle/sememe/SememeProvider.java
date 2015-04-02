/*
 * Copyright 2015 kec.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gov.vha.isaac.cradle.sememe;

import gov.vha.isaac.cradle.IsaacDbFolder;
import gov.vha.isaac.cradle.waitfree.CasSequenceObjectMap;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.SequenceService;
import gov.vha.isaac.ochre.api.coordinate.StampCoordinate;
import gov.vha.isaac.ochre.api.sememe.SememeChronicle;
import gov.vha.isaac.ochre.api.sememe.SememeService;
import gov.vha.isaac.ochre.api.sememe.SememeSnapshotService;
import gov.vha.isaac.ochre.api.sememe.version.SememeVersion;
import gov.vha.isaac.ochre.collections.SememeSequenceSet;
import gov.vha.isaac.ochre.model.sememe.SememeChronicleImpl;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Stream;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.glassfish.hk2.runlevel.RunLevel;
import org.jvnet.hk2.annotations.Service;

/**
 *
 * @author kec
 */
@Service
@RunLevel(value = 0)
public class SememeProvider implements SememeService {

    private static final Logger log = LogManager.getLogger();

    final CasSequenceObjectMap<SememeChronicleImpl<?>> sememeMap;
    final ConcurrentSkipListSet<SememeKey> assemblageNidReferencedNidSememeSequenceMap = new ConcurrentSkipListSet<>();
    final ConcurrentSkipListSet<SememeKey> referencedNidAssemblageNidSememeSequenceMap = new ConcurrentSkipListSet<>();
    final SequenceService sequenceProvider;

    public SememeProvider() throws IOException {
        sequenceProvider = LookupService.getService(SequenceService.class);
        sememeMap = new CasSequenceObjectMap(new SememeSerializer());
    }

    @PostConstruct
    private void startMe() throws IOException {
        log.info("Loading sememeMap.");
        if (!IsaacDbFolder.get().getPrimordial()) {
            log.info("Reading taxonomy.");
            sememeMap.read(IsaacDbFolder.get().getDbFolderPath(), "sememe/", ".sememe.map");

            log.info("Loading SememeKeys.");

            try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(new File(IsaacDbFolder.get().getDbFolderPath().toFile(), "sememe.keys"))))) {
                int size = in.readInt();
                for (int i = 0; i < size; i++) {
                    int key1 = in.readInt();
                    int key2 = in.readInt();
                    int sequence = in.readInt();
                    assemblageNidReferencedNidSememeSequenceMap.add(new SememeKey(key1, key2, sequence));
                    referencedNidAssemblageNidSememeSequenceMap.add(new SememeKey(key2, key1, sequence));
                }
            }
        }
        log.info("Finished SememeProvider load.");
    }

    @PreDestroy
    private void stopMe() throws IOException {
        log.info("Stopping SememeProvider pre-destroy. ");

        log.info("sememeMap size: {}", sememeMap.getSize());
        log.info("writing sememe-map.");
        sememeMap.write(IsaacDbFolder.get().getDbFolderPath(), "sememe/", ".sememe.map");

        log.info("writing SememeKeys.");
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(IsaacDbFolder.get().getDbFolderPath().toFile(), "sememe.keys"))))) {
            out.writeInt(assemblageNidReferencedNidSememeSequenceMap.size());
            for (SememeKey key : assemblageNidReferencedNidSememeSequenceMap) {
                out.writeInt(key.key1);
                out.writeInt(key.key2);
                out.writeInt(key.sememeSequence);
            }
        }
        log.info("Finished SememeProvider stop.");
    }

    @Override
    public <V extends SememeVersion> SememeSnapshotService<V> getSnapshot(Class<V> versionType, StampCoordinate stampCoordinate) {
        return new SememeSnapshotProvider<>(versionType, stampCoordinate, this);
    }

    @Override
    public SememeChronicle getSememe(int sememeSequence) {
        sememeSequence = sequenceProvider.getSememeSequence(sememeSequence);
        return sememeMap.getQuick(sememeSequence);
    }

    @Override
    public Stream<SememeChronicle> getSememesFromAssemblage(int assemblageSequence) {
        SememeSequenceSet sememeSequences = getSememeSequencesFromAssemblage(assemblageSequence);
        return sememeSequences.stream().mapToObj((int sememeSequence) -> getSememe(sememeSequence));
    }

    @Override
    public SememeSequenceSet getSememeSequencesFromAssemblage(int assemblageSequence) {
        assemblageSequence = sequenceProvider.getSememeSequence(assemblageSequence);
        SememeKey rangeStart = new SememeKey(assemblageSequence, Integer.MIN_VALUE, Integer.MIN_VALUE); // yes
        SememeKey rangeEnd = new SememeKey(assemblageSequence, Integer.MAX_VALUE, Integer.MAX_VALUE); // no
        NavigableSet<SememeKey> assemblageSememeKeys
                = assemblageNidReferencedNidSememeSequenceMap.subSet(rangeStart, true,
                        rangeEnd, true
                );
        return SememeSequenceSet.of(assemblageSememeKeys.stream().mapToInt((SememeKey key) -> key.sememeSequence));
    }

    @Override
    public Stream<SememeChronicle> getSememesForComponent(int componentNid) {
        SememeSequenceSet sememeSequences = getSememeSequencesForComponent(componentNid);
        return sememeSequences.stream().mapToObj((int sememeSequence) -> getSememe(sememeSequence));
    }

    @Override
    public SememeSequenceSet getSememeSequencesForComponent(int componentNid) {
        if (componentNid >= 0) {
            throw new IndexOutOfBoundsException("Component identifiers must be negative. Found: " + componentNid);
        }
        NavigableSet<SememeKey> assemblageSememeKeys
                = referencedNidAssemblageNidSememeSequenceMap.subSet(
                        new SememeKey(componentNid, Integer.MIN_VALUE, Integer.MIN_VALUE), true,
                        new SememeKey(componentNid, Integer.MAX_VALUE, Integer.MAX_VALUE), true
                );
        return SememeSequenceSet.of(assemblageSememeKeys.stream().mapToInt((SememeKey key) -> key.sememeSequence));
    }

    @Override
    public Stream<SememeChronicle> getSememesForComponentFromAssemblage(int componentNid, int assemblageSequence) {
        if (componentNid >= 0) {
            componentNid = sequenceProvider.getConceptNid(componentNid);
        }
        if (assemblageSequence < 0) {
            assemblageSequence = sequenceProvider.getConceptSequence(assemblageSequence);
        }
        SememeSequenceSet sememeSequences = getSememeSequencesForComponentFromAssemblage(componentNid, assemblageSequence);
        return sememeSequences.stream().mapToObj((int sememeSequence) -> getSememe(sememeSequence));
    }

    @Override
    public SememeSequenceSet getSememeSequencesForComponentFromAssemblage(int componentNid, int assemblageSequence) {
        if (componentNid >= 0) {
            throw new IndexOutOfBoundsException("Component identifiers must be negative. Found: " + componentNid);
        }
        assemblageSequence = sequenceProvider.getSememeSequence(assemblageSequence);
        SememeKey rangeStart = new SememeKey(assemblageSequence, componentNid, Integer.MIN_VALUE); // yes
        SememeKey rangeEnd = new SememeKey(assemblageSequence, componentNid, Integer.MAX_VALUE); // no
        NavigableSet<SememeKey> assemblageSememeKeys
                = assemblageNidReferencedNidSememeSequenceMap.subSet(rangeStart, true,
                        rangeEnd, true
                );
        return SememeSequenceSet.of(assemblageSememeKeys.stream().mapToInt((SememeKey key) -> key.sememeSequence));
    }

    @Override
    public void writeSememe(SememeChronicle sememeChronicle) {
        sememeMap.put(sememeChronicle.getSememeSequence(), 
                (SememeChronicleImpl<?>) sememeChronicle);
    }
}
