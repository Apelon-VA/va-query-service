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
package gov.vha.isaac.cradle.refex;

import org.ihtsdo.otf.tcc.model.cc.refex.RefexService;
import gov.vha.isaac.cradle.Cradle;
import gov.vha.isaac.cradle.collections.ConcurrentSequenceSerializedObjectMap;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.IdentifierService;
import gov.vha.isaac.ochre.api.SystemStatusService;
import gov.vha.isaac.ochre.collections.RefexSequenceSet;
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
import org.ihtsdo.otf.tcc.api.chronicle.ComponentChronicleBI;
import org.ihtsdo.otf.tcc.api.refexDynamic.RefexDynamicChronicleBI;
import org.ihtsdo.otf.tcc.model.cc.NidPairForRefex;
import org.ihtsdo.otf.tcc.model.cc.refex.RefexMember;
import org.jvnet.hk2.annotations.Service;

/**
 * TODO convert to CasSequenceObjectMap
 * @author kec
 */
@Service
@RunLevel(value = 0)
public class RefexProvider implements RefexService {

    private static final Logger log = LogManager.getLogger();

    final ConcurrentSequenceSerializedObjectMap<ComponentChronicleBI<?>> refexMap;
    final ConcurrentSkipListSet<RefexKey> assemblageSequenceRefexSequenceMap = new ConcurrentSkipListSet<>();
    final ConcurrentSkipListSet<RefexKey> referencedNidRefexSequenceMap = new ConcurrentSkipListSet<>();
    final ConcurrentSkipListSet<Integer> dynamicMembers = new ConcurrentSkipListSet<>();
    final IdentifierService sequenceProvider;

    //For HK2 only
    private RefexProvider() throws IOException {
        try {
            sequenceProvider = LookupService.getService(IdentifierService.class);
            
            Path refexMapPath = Cradle.getCradlePath().resolve(REFEX_MAP);
            log.info("Starting RefexProvider - using from " + refexMapPath.toAbsolutePath().toString());
            
            refexMap = new ConcurrentSequenceSerializedObjectMap(new RefexSerializer(), refexMapPath, "seg.", ".refex.map");
        }
        catch (Exception e) {
            LookupService.getService(SystemStatusService.class).notifyServiceConfigurationFailure("Refex Provider", e);
            throw e;
        }
    }
    private static final String REFEX_MAP = "refex-map";
    private static final String ASSEMBLAGE_REFEX_KEYS = "assemblage-refex.keys";
    private static final String COMPONENT_REFEX_KEYS = "component-refex.keys";
    private static final String DYNAMIC_REFEX_KEYS = "dynamic-refex.keys";

    @PostConstruct
    private void startMe() throws IOException {
        try {
            if (!Cradle.cradleStartedEmpty()) {
                log.info("Loading refexMap.");
                refexMap.read();
    
                log.info("Loading RefexKeys.");
    
                try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(new File(Cradle.getCradlePath().toFile(), ASSEMBLAGE_REFEX_KEYS))))) {
                    int size = in.readInt();
                    for (int i = 0; i < size; i++) {
                        int key1 = in.readInt();
                        int sequence = in.readInt();
                        assemblageSequenceRefexSequenceMap.add(new RefexKey(key1, sequence));
                    }
                }
                try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(new File(Cradle.getCradlePath().toFile(), COMPONENT_REFEX_KEYS))))) {
                    int size = in.readInt();
                    for (int i = 0; i < size; i++) {
                        int key1 = in.readInt();
                        int sequence = in.readInt();
                        referencedNidRefexSequenceMap.add(new RefexKey(key1, sequence));
                    }
                }
                try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(new File(Cradle.getCradlePath().toFile(), DYNAMIC_REFEX_KEYS))))) {
                    int size = in.readInt();
                    for (int i = 0; i < size; i++) {
                        int sequence = in.readInt();
                        dynamicMembers.add(sequence);
                    }
                }
                log.info("Finished RefexProvider load.");
            }
        }
        catch (Exception e) {
            LookupService.getService(SystemStatusService.class).notifyServiceConfigurationFailure("Refex Provider", e);
            throw e;
        }

    }

    @PreDestroy
    private void stopMe() throws IOException {
        log.info("Stopping RefexProvider pre-destroy. ");

        log.info("refexMap size: {}", refexMap.getSize());
        log.info("writing refex-map.");
        refexMap.write();

        log.info("writing RefexKeys.");
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(Cradle.getCradlePath().toFile(), ASSEMBLAGE_REFEX_KEYS))))) {
            out.writeInt(assemblageSequenceRefexSequenceMap.size());
            for (RefexKey key : assemblageSequenceRefexSequenceMap) {
                out.writeInt(key.key1);
                out.writeInt(key.refexSequence);
            }
        }        
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(Cradle.getCradlePath().toFile(), COMPONENT_REFEX_KEYS))))) {
            out.writeInt(referencedNidRefexSequenceMap.size());
            for (RefexKey key : referencedNidRefexSequenceMap) {
                out.writeInt(key.key1);
                out.writeInt(key.refexSequence);
            }
        }
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(Cradle.getCradlePath().toFile(), DYNAMIC_REFEX_KEYS))))) {
            out.writeInt(dynamicMembers.size());
            for (int sequence : dynamicMembers) {
                out.writeInt(sequence);
            }
        }
        log.info("Finished RefexProvider stop.");
    }

    @Override
    public RefexMember<?, ?> getRefex(int refexSequence) {
        refexSequence = sequenceProvider.getRefexSequence(refexSequence);
        return castConceptChronicleToRefexMember(refexMap.getQuick(refexSequence));
    }

    private static RefexMember<?, ?> castConceptChronicleToRefexMember(ComponentChronicleBI<?> cc) {
        if (cc != null && !(cc instanceof RefexMember)) {
            log.error("Attempting cast of {} \"{}\" to RefexMember", cc.getClass().getName(), cc);
        }

        return (RefexMember<?, ?>)cc;
    }

    @Override
    public Stream<RefexMember<?, ?>> getRefexesFromAssemblage(int assemblageSequence) {
        RefexSequenceSet refexSequences = getRefexSequencesFromAssemblage(assemblageSequence);
        return refexSequences.stream().mapToObj((int value) -> castConceptChronicleToRefexMember(refexMap.getQuick(value)));
    }

    @Override
    public RefexSequenceSet getRefexSequencesFromAssemblage(int assemblageSequence) {
        assemblageSequence = sequenceProvider.getRefexSequence(assemblageSequence);
        RefexKey rangeStart = new RefexKey(assemblageSequence, Integer.MIN_VALUE); // yes
        RefexKey rangeEnd = new RefexKey(assemblageSequence, Integer.MAX_VALUE); // no
        NavigableSet<RefexKey> assemblageRefexKeys
                = assemblageSequenceRefexSequenceMap.subSet(rangeStart, true,
                        rangeEnd, true
                );
        return RefexSequenceSet.of(assemblageRefexKeys.stream().mapToInt((RefexKey key) -> key.refexSequence));
    }

    @Override
    public Stream<RefexMember<?, ?>> getRefexesForComponent(int componentNid) {
        RefexSequenceSet refexSequences = getRefexSequencesForComponent(componentNid);
        return refexSequences.stream().mapToObj((refexSequence)-> getRefex(refexSequence));
        
    }

    @Override
    public RefexSequenceSet getRefexSequencesForComponent(int componentNid) {
        if (componentNid >= 0) {
            throw new IndexOutOfBoundsException("Component identifiers must be negative. Found: " + componentNid);
        }
        NavigableSet<RefexKey> assemblageRefexKeys
                = referencedNidRefexSequenceMap.subSet(
                        new RefexKey(componentNid, Integer.MIN_VALUE), true,
                        new RefexKey(componentNid, Integer.MAX_VALUE), true
                );
        return RefexSequenceSet.of(assemblageRefexKeys.stream().mapToInt((RefexKey key) -> key.refexSequence));
    }

    @Override
    public Stream<RefexMember<?, ?>> getRefexesForComponentFromAssemblage(int componentNid, int assemblageSequence) {
        RefexSequenceSet refexSequences = getRefexSequencesForComponentFromAssemblage(componentNid, assemblageSequence);
        return refexSequences.stream().mapToObj((refexSequence)-> getRefex(refexSequence));
    }

    @Override
    public RefexSequenceSet getRefexSequencesForComponentFromAssemblage(int componentNid, int assemblageSequence) {
        if (componentNid >= 0) {
            throw new IndexOutOfBoundsException("Component identifiers must be negative. Found: " + componentNid);
        }
        assemblageSequence = sequenceProvider.getRefexSequence(assemblageSequence);
        RefexKey rangeStart = new RefexKey(assemblageSequence, Integer.MIN_VALUE); // yes
        RefexKey rangeEnd = new RefexKey(assemblageSequence, Integer.MAX_VALUE); // no
        NavigableSet<RefexKey> assemblageRefexKeys
                = assemblageSequenceRefexSequenceMap.subSet(rangeStart, true,
                        rangeEnd, true
                );
        RefexKey rcRangeStart = new RefexKey(componentNid, Integer.MIN_VALUE); // yes
        RefexKey rcRangeEnd = new RefexKey(componentNid, Integer.MAX_VALUE); // no
        NavigableSet<RefexKey> referencedComponentRefexKeys
                = referencedNidRefexSequenceMap.subSet(rcRangeStart, true,
                        rcRangeEnd, true
                );
        RefexSequenceSet assemblageSet = RefexSequenceSet.of(assemblageRefexKeys.stream().mapToInt((RefexKey key) -> key.refexSequence));
        RefexSequenceSet referencedComponentSet = RefexSequenceSet.of(referencedComponentRefexKeys.stream().mapToInt((RefexKey key) -> key.refexSequence));
        assemblageSet.and(referencedComponentSet);
        return assemblageSet;
    }


    @Override
    public Stream<RefexMember<?, ?>> getRefexStream() {
        return refexMap.getStream()
                .filter((component) ->{return component instanceof RefexMember;})
                .map((component) -> {return castConceptChronicleToRefexMember(component);});
    }

    @Override
    public Stream<RefexMember<?, ?>> getParallelRefexStream() {
        return refexMap.getParallelStream()
                .filter((component) ->{return component instanceof RefexMember;})
                .map((component) -> {return castConceptChronicleToRefexMember(component);});
    }

    @Override
    public void forgetXrefPair(int referencedComponentNid, NidPairForRefex nidPairForRefex) {
        int sequence = sequenceProvider.getSememeSequence(nidPairForRefex.getMemberNid());
        int assemblageSequence = sequenceProvider.getRefexSequence(nidPairForRefex.getRefexNid());
        assemblageSequenceRefexSequenceMap.remove(new RefexKey(assemblageSequence, sequence));
        referencedNidRefexSequenceMap.remove(new RefexKey(referencedComponentNid, sequence));
    }

    @Override
    public Stream<RefexDynamicChronicleBI<?>> getDynamicRefexesForComponent(int componentNid) {
        return getRefexSequencesForComponent(componentNid).stream()
                .filter((sequence) -> dynamicMembers.contains(sequence))
                .mapToObj((sequence) -> (RefexDynamicChronicleBI<?>) refexMap.getQuick(sequence));
    }

    @Override
    public Stream<RefexDynamicChronicleBI<?>> getDynamicRefexesFromAssemblage(int assemblageSequence) {
        return getRefexSequencesFromAssemblage(assemblageSequence).stream()
                .filter((sequence) -> dynamicMembers.contains(sequence))
                .mapToObj((sequence) -> (RefexDynamicChronicleBI<?>) refexMap.getQuick(sequence));
    }

    @Override
    public void writeDynamicRefex(RefexDynamicChronicleBI<?> refex) {
        int sequence = sequenceProvider.getRefexSequence(refex.getNid());
        int assemblageSequence = sequenceProvider.getRefexSequence(refex.getAssemblageNid());
        if (!refexMap.containsKey(sequence)) {
            assemblageSequenceRefexSequenceMap.add(new RefexKey(assemblageSequence,
                    sequence));
            referencedNidRefexSequenceMap.add(new RefexKey(refex.getReferencedComponentNid(),
                    sequence));
            dynamicMembers.add(sequence);
        }
        refexMap.put(sequence, refex);
    }
    
    @Override
    public void writeRefex(RefexMember<?, ?> refex) {
        int sequence = sequenceProvider.getRefexSequence(refex.getNid());
        int assemblageSequence = sequenceProvider.getRefexSequence(refex.assemblageNid);
        if (!refexMap.containsKey(sequence)) {
            assemblageSequenceRefexSequenceMap.add(new RefexKey(assemblageSequence,
                    sequence));
            referencedNidRefexSequenceMap.add(new RefexKey(refex.referencedComponentNid,
                    sequence));
        }
        refexMap.put(sequence, refex);
    }

}
