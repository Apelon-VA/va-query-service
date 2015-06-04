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

import gov.vha.isaac.cradle.Cradle;
import gov.vha.isaac.cradle.waitfree.CasSequenceObjectMap;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.IdentifierService;
import gov.vha.isaac.ochre.api.SystemStatusService;
import gov.vha.isaac.ochre.api.commit.CommitService;
import gov.vha.isaac.ochre.api.coordinate.StampCoordinate;
import gov.vha.isaac.ochre.api.coordinate.StampPosition;
import gov.vha.isaac.ochre.api.component.sememe.SememeChronology;
import gov.vha.isaac.ochre.api.component.sememe.SememeService;
import gov.vha.isaac.ochre.api.component.sememe.SememeSnapshotService;
import gov.vha.isaac.ochre.api.component.sememe.version.SememeVersion;
import gov.vha.isaac.ochre.collections.NidSet;
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
import java.nio.file.Path;
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

    private static CommitService commitService;

    private static CommitService getCommitService() {
        if (commitService == null) {
            commitService = LookupService.getService(CommitService.class);
        }
        return commitService;
    }

    final CasSequenceObjectMap<SememeChronicleImpl<?>> sememeMap;
    final ConcurrentSkipListSet<SememeKey> assemblageSequenceSememeSequenceMap = new ConcurrentSkipListSet<>();
    final ConcurrentSkipListSet<SememeKey> referencedNidSememeSequenceMap = new ConcurrentSkipListSet<>();
    final IdentifierService identifierService;

    //For HK2
    private SememeProvider() throws IOException {
        try {
            identifierService = LookupService.getService(IdentifierService.class);

            Path sememePath = Cradle.getCradlePath().resolve("sememe");
            log.info("Setting up sememe provider at " + sememePath.toAbsolutePath().toString());

            sememeMap = new CasSequenceObjectMap(new SememeSerializer(), sememePath, "seg.", ".sememe.map");
        } catch (Exception e) {
            LookupService.getService(SystemStatusService.class).notifyServiceConfigurationFailure("Cradle Commit Manager", e);
            throw e;
        }
    }

    @PostConstruct
    private void startMe() throws IOException {
        try {
            log.info("Loading sememeMap.");
            if (!Cradle.cradleStartedEmpty()) {
                log.info("Reading sememeMap.");
                sememeMap.initialize();

                log.info("Loading SememeKeys.");

                try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(new File(Cradle.getCradlePath().toFile(), "assemblage-sememe.keys"))))) {
                    int size = in.readInt();
                    for (int i = 0; i < size; i++) {
                        int key1 = in.readInt();
                        int sequence = in.readInt();
                        assemblageSequenceSememeSequenceMap.add(new SememeKey(key1, sequence));
                    }
                }
                try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(new File(Cradle.getCradlePath().toFile(), "component-sememe.keys"))))) {
                    int size = in.readInt();
                    for (int i = 0; i < size; i++) {
                        int key1 = in.readInt();
                        int sequence = in.readInt();
                        referencedNidSememeSequenceMap.add(new SememeKey(key1, sequence));
                    }
                }
            }

            SememeSequenceSet statedGraphSequences = getSememeSequencesFromAssemblage(identifierService.getConceptSequence(identifierService.getNidForUuids(IsaacMetadataAuxiliaryBinding.EL_PLUS_PLUS_STATED_FORM.getUuids())));
            log.info("Stated logic graphs: " + statedGraphSequences.size());

            SememeSequenceSet inferedGraphSequences = getSememeSequencesFromAssemblage(identifierService.getConceptSequence(identifierService.getNidForUuids(IsaacMetadataAuxiliaryBinding.EL_PLUS_PLUS_INFERRED_FORM.getUuids())));

            log.info("Inferred logic graphs: " + inferedGraphSequences.size());
            log.info("Finished SememeProvider load.");
        } catch (Exception e) {
            LookupService.getService(SystemStatusService.class).notifyServiceConfigurationFailure("Cradle Commit Manager", e);
            throw e;
        }
    }

    @PreDestroy
    private void stopMe() throws IOException {
        log.info("Stopping SememeProvider pre-destroy. ");

        //Dan commented out this log statement because it is really slow...
        //log.info("sememeMap size: {}", sememeMap.getSize());
        log.info("writing sememe-map.");
        sememeMap.write();

        log.info("writing SememeKeys.");
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(Cradle.getCradlePath().toFile(), "assemblage-sememe.keys"))))) {
            out.writeInt(assemblageSequenceSememeSequenceMap.size());
            for (SememeKey key : assemblageSequenceSememeSequenceMap) {
                out.writeInt(key.key1);
                out.writeInt(key.sememeSequence);
            }
        }
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(Cradle.getCradlePath().toFile(), "component-sememe.keys"))))) {
            out.writeInt(referencedNidSememeSequenceMap.size());
            for (SememeKey key : referencedNidSememeSequenceMap) {
                out.writeInt(key.key1);
                out.writeInt(key.sememeSequence);
            }
        }
        SememeSequenceSet statedGraphSequences = getSememeSequencesFromAssemblage(IsaacMetadataAuxiliaryBinding.EL_PLUS_PLUS_STATED_FORM.getSequence());
        log.info("Stated logic graphs: " + statedGraphSequences.size());
        SememeSequenceSet inferedGraphSequences = getSememeSequencesFromAssemblage(IsaacMetadataAuxiliaryBinding.EL_PLUS_PLUS_INFERRED_FORM.getSequence());
        log.info("Inferred logic graphs: " + inferedGraphSequences.size());
        log.info("Finished SememeProvider stop.");
    }

    @Override
    public <V extends SememeVersion> SememeSnapshotService<V> getSnapshot(Class<V> versionType, StampCoordinate stampCoordinate) {
        return new SememeSnapshotProvider<>(versionType, stampCoordinate, this);
    }

    @Override
    public SememeChronology getSememe(int sememeSequence) {
        sememeSequence = identifierService.getSememeSequence(sememeSequence);
        return sememeMap.getQuick(sememeSequence);
    }

    @Override
    public Stream<SememeChronology<? extends SememeVersion>> getSememesFromAssemblage(int assemblageSequence) {
        SememeSequenceSet sememeSequences = getSememeSequencesFromAssemblage(assemblageSequence);
        return sememeSequences.stream().mapToObj((int sememeSequence) -> getSememe(sememeSequence));
    }

    @Override
    public SememeSequenceSet getSememeSequencesFromAssemblage(int assemblageSequence) {
        assemblageSequence = identifierService.getSememeSequence(assemblageSequence);
        SememeKey rangeStart = new SememeKey(assemblageSequence, Integer.MIN_VALUE); // yes
        SememeKey rangeEnd = new SememeKey(assemblageSequence, Integer.MAX_VALUE); // no
        NavigableSet<SememeKey> assemblageSememeKeys
                = assemblageSequenceSememeSequenceMap.subSet(rangeStart, true,
                        rangeEnd, true
                );
        return SememeSequenceSet.of(assemblageSememeKeys.stream().mapToInt((SememeKey key) -> key.sememeSequence));
    }

    @Override
    public Stream<SememeChronology<? extends SememeVersion>> getSememesForComponent(int componentNid) {
        SememeSequenceSet sememeSequences = getSememeSequencesForComponent(componentNid);
        return sememeSequences.stream().mapToObj((int sememeSequence) -> getSememe(sememeSequence));
    }

    @Override
    public SememeSequenceSet getSememeSequencesForComponent(int componentNid) {
        if (componentNid >= 0) {
            throw new IndexOutOfBoundsException("Component identifiers must be negative. Found: " + componentNid);
        }
        NavigableSet<SememeKey> assemblageSememeKeys
                = referencedNidSememeSequenceMap.subSet(
                        new SememeKey(componentNid, Integer.MIN_VALUE), true,
                        new SememeKey(componentNid, Integer.MAX_VALUE), true
                );
        return SememeSequenceSet.of(assemblageSememeKeys.stream().mapToInt((SememeKey key) -> key.sememeSequence));
    }

    @Override
    public Stream<SememeChronology<? extends SememeVersion>> getSememesForComponentFromAssemblage(int componentNid, int assemblageSequence) {
        if (componentNid >= 0) {
            componentNid = identifierService.getConceptNid(componentNid);
        }
        if (assemblageSequence < 0) {
            assemblageSequence = identifierService.getConceptSequence(assemblageSequence);
        }
        SememeSequenceSet sememeSequences = getSememeSequencesForComponentFromAssemblage(componentNid, assemblageSequence);
        return sememeSequences.stream().mapToObj((int sememeSequence) -> getSememe(sememeSequence));
    }

    @Override
    public SememeSequenceSet getSememeSequencesForComponentFromAssemblage(int componentNid, int assemblageSequence) {
        if (componentNid >= 0) {
            throw new IndexOutOfBoundsException("Component identifiers must be negative. Found: " + componentNid);
        }
        assemblageSequence = identifierService.getSememeSequence(assemblageSequence);
        SememeKey rangeStart = new SememeKey(assemblageSequence, Integer.MIN_VALUE); // yes
        SememeKey rangeEnd = new SememeKey(assemblageSequence, Integer.MAX_VALUE); // no
        NavigableSet<SememeKey> assemblageRefexKeys
                = assemblageSequenceSememeSequenceMap.subSet(rangeStart, true,
                        rangeEnd, true
                );
        SememeKey rcRangeStart = new SememeKey(componentNid, Integer.MIN_VALUE); // yes
        SememeKey rcRangeEnd = new SememeKey(componentNid, Integer.MAX_VALUE); // no
        NavigableSet<SememeKey> referencedComponentRefexKeys
                = referencedNidSememeSequenceMap.subSet(rcRangeStart, true,
                        rcRangeEnd, true
                );
        SememeSequenceSet assemblageSet = SememeSequenceSet.of(assemblageRefexKeys.stream().mapToInt((SememeKey key) -> key.sememeSequence));
        SememeSequenceSet referencedComponentSet = SememeSequenceSet.of(referencedComponentRefexKeys.stream().mapToInt((SememeKey key) -> key.sememeSequence));
        assemblageSet.and(referencedComponentSet);
        return assemblageSet;
    }

    @Override
    public SememeSequenceSet getSememeSequencesForComponentsFromAssemblage(NidSet componentNidSet, int assemblageSequence) {
        assemblageSequence = identifierService.getSememeSequence(assemblageSequence);
        SememeKey rangeStart = new SememeKey(assemblageSequence, Integer.MIN_VALUE); // yes
        SememeKey rangeEnd = new SememeKey(assemblageSequence, Integer.MAX_VALUE); // no
        NavigableSet<SememeKey> assemblageRefexKeys
                = assemblageSequenceSememeSequenceMap.subSet(rangeStart, true,
                        rangeEnd, true
                );

        SememeSequenceSet referencedComponentSet = new SememeSequenceSet();
        componentNidSet.stream().forEach((componentNid) -> {
            SememeKey rcRangeStart = new SememeKey(componentNid, Integer.MIN_VALUE); // yes
            SememeKey rcRangeEnd = new SememeKey(componentNid, Integer.MAX_VALUE); // no
            NavigableSet<SememeKey> referencedComponentRefexKeys
                    = referencedNidSememeSequenceMap.subSet(rcRangeStart, true,
                            rcRangeEnd, true
                    );
            referencedComponentSet.or(SememeSequenceSet.of(referencedComponentRefexKeys.stream().mapToInt((SememeKey key) -> key.sememeSequence)));
        });

        SememeSequenceSet assemblageSet = SememeSequenceSet.of(assemblageRefexKeys.stream().mapToInt((SememeKey key) -> key.sememeSequence));
        assemblageSet.and(referencedComponentSet);
        return assemblageSet;
    }

    @Override
    public void writeSememe(SememeChronology sememeChronicle) {
        assemblageSequenceSememeSequenceMap.add(
                new SememeKey(sememeChronicle.getAssemblageSequence(),
                        sememeChronicle.getSememeSequence()));
        referencedNidSememeSequenceMap.add(
                new SememeKey(sememeChronicle.getReferencedComponentNid(),
                        sememeChronicle.getSememeSequence()));
        sememeMap.put(sememeChronicle.getSememeSequence(),
                (SememeChronicleImpl<?>) sememeChronicle);
    }

    @Override
    public SememeSequenceSet getSememeSequencesForComponentsFromAssemblageModifiedAfterPosition(
            NidSet componentNidSet, int assemblageSequence, StampPosition position) {
        SememeSequenceSet sequencesToTest = 
                getSememeSequencesForComponentsFromAssemblage(componentNidSet, assemblageSequence);
        SememeSequenceSet sequencesThatPassedTest = new SememeSequenceSet();
        getCommitService();
        sequencesToTest.stream().forEach((sememeSequence) -> {
            SememeChronicleImpl<?> chronicle = (SememeChronicleImpl<?>) getSememe(sememeSequence);
            if (chronicle.getVersionStampSequences().anyMatch((stampSequence) -> {
                if ((commitService.getTimeForStamp(stampSequence) > position.getTime()
                        && (position.getStampPathSequence() == commitService.getPathSequenceForStamp(stampSequence)))) {
                    return true;
                }
                return false;
            })) {
                sequencesThatPassedTest.add(sememeSequence);
            }
        });
        return sequencesThatPassedTest;
    }

    @Override
    public SememeSequenceSet getSememeSequencesFromAssemblageModifiedAfterPosition(int assemblageSequence, StampPosition position) {
        SememeSequenceSet sequencesToTest = getSememeSequencesFromAssemblage(assemblageSequence);
        SememeSequenceSet sequencesThatPassedTest = new SememeSequenceSet();
        getCommitService();
        sequencesToTest.stream().forEach((sememeSequence) -> {
            SememeChronicleImpl<?> chronicle = (SememeChronicleImpl<?>) getSememe(sememeSequence);
            if (chronicle.getVersionStampSequences().anyMatch((stampSequence) -> {
                if ((commitService.getTimeForStamp(stampSequence) > position.getTime()
                        && (position.getStampPathSequence() == commitService.getPathSequenceForStamp(stampSequence)))) {
                    return true;
                }
                return false;
            })) {
                sequencesThatPassedTest.add(sememeSequence);
            }
        });
        return sequencesThatPassedTest;
    }

    @Override
    public Stream<SememeChronology<? extends SememeVersion>> getSememeStream() {
        return identifierService.getSememeSequenceStream().mapToObj((int sememeSequence) -> getSememe(sememeSequence));
    }

    @Override
    public Stream<SememeChronology<? extends SememeVersion>> getParallelSememeStream() {
        return identifierService.getSememeSequenceStream().parallel().mapToObj((int sememeSequence) -> getSememe(sememeSequence));
    }

}
