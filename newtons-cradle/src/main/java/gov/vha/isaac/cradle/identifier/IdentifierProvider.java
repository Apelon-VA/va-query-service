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
package gov.vha.isaac.cradle.identifier;

import gov.vha.isaac.cradle.ConcurrentSequenceIntMap;
import gov.vha.isaac.cradle.IsaacDbFolder;
import gov.vha.isaac.cradle.collections.SequenceMap;
import gov.vha.isaac.cradle.collections.UuidIntMapMap;
import gov.vha.isaac.ochre.api.IdentifiedObjectService;
import gov.vha.isaac.ochre.api.IdentifierService;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.chronicle.IdentifiedObjectLocal;
import gov.vha.isaac.ochre.api.sememe.SememeChronicle;
import gov.vha.isaac.ochre.api.sememe.SememeService;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import gov.vha.isaac.ochre.collections.NidSet;
import gov.vha.isaac.ochre.collections.RefexSequenceSet;
import gov.vha.isaac.ochre.collections.SememeSequenceSet;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.stream.IntStream;
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
public class IdentifierProvider implements IdentifierService {

    private static final Logger log = LogManager.getLogger();

    private static SememeService sememeProvider = null;

    private SememeService getSememeService() {
        if (sememeProvider == null) {
            sememeProvider = LookupService.getService(SememeService.class);
        }
        return sememeProvider;
    }
    private static IdentifiedObjectService ios = null;

    private static IdentifiedObjectService getIdentifiedObjectService() {
        if (ios == null) {
            ios = LookupService.getService(IdentifiedObjectService.class);
        }
        return ios;
    }

    final UuidIntMapMap uuidIntMapMap = UuidIntMapMap.create(new File(IsaacDbFolder.get().getDbFolderPath().toFile(), "uuid-nid-map"));
    final SequenceMap conceptSequenceMap = new SequenceMap(450000);
    final SequenceMap sememeSequenceMap = new SequenceMap(3000000);
    final SequenceMap refexSequenceMap = new SequenceMap(3000000);
    final ConcurrentSequenceIntMap nidCnidMap = new ConcurrentSequenceIntMap();

    @PostConstruct
    private void startMe() throws IOException {
        log.info("Starting SequenceService post-construct");
        if (!IsaacDbFolder.get().getPrimordial()) {
            log.info("Loading concept-sequence.map.");
            conceptSequenceMap.read(new File(IsaacDbFolder.get().getDbFolderPath().toFile(), "concept-sequence.map"));
            log.info("Loading sememe-sequence.map.");
            sememeSequenceMap.read(new File(IsaacDbFolder.get().getDbFolderPath().toFile(), "sememe-sequence.map"));
            log.info("Loading refex-sequence.map.");
            refexSequenceMap.read(new File(IsaacDbFolder.get().getDbFolderPath().toFile(), "refex-sequence.map"));
            // uuid-nid-map can do dynamic load, no need to read all at the beginning.
            // log.info("Loading uuid-nid-map.");
            // uuidIntMapMap.read();
            log.info("Loading sequence-cnid-map.");
            nidCnidMap.read(new File(IsaacDbFolder.get().getDbFolderPath().toFile(), "sequence-cnid-map"));
        }
    }

    @PreDestroy
    private void stopMe() throws IOException {
        uuidIntMapMap.setShutdown(true);
        log.info("conceptSequence: {}", conceptSequenceMap.getNextSequence());
        log.info("writing concept-sequence.map.");
        conceptSequenceMap.write(new File(IsaacDbFolder.get().getDbFolderPath().toFile(), "concept-sequence.map"));
        log.info("writing sememe-sequence.map.");
        sememeSequenceMap.write(new File(IsaacDbFolder.get().getDbFolderPath().toFile(), "sememe-sequence.map"));
        log.info("writing refex-sequence.map.");
        refexSequenceMap.write(new File(IsaacDbFolder.get().getDbFolderPath().toFile(), "refex-sequence.map"));
        log.info("writing uuid-nid-map.");
        uuidIntMapMap.write();
        log.info("writing sequence-cnid-map.");
        nidCnidMap.write(new File(IsaacDbFolder.get().getDbFolderPath().toFile(), "sequence-cnid-map"));
    }

    @Override
    public int getConceptSequence(int nid) {
        if (nid >= 0) {
            return nid;
        }
        return conceptSequenceMap.addNidIfMissing(nid);
    }

    @Override
    public int getConceptNid(int conceptSequence) {
        if (conceptSequence < 0) {
            return conceptSequence;
        }
        return conceptSequenceMap.getNidFast(conceptSequence);
    }

    @Override
    public int getSememeSequence(int nid) {
        if (nid >= 0) {
            return nid;
        }
        return sememeSequenceMap.addNidIfMissing(nid);
    }

    @Override
    public int getSememeNid(int sememeSequence) {
        if (sememeSequence < 0) {
            return sememeSequence;
        }
        return sememeSequenceMap.getNidFast(sememeSequence);
    }

    @Override
    public IntStream getConceptSequenceStream() {
        return conceptSequenceMap.getSequenceStream();
    }

    @Override
    public IntStream getParallelConceptSequenceStream() {
        return conceptSequenceMap.getSequenceStream().parallel();
    }

    @Override
    public IntStream getSememeSequenceStream() {
        return sememeSequenceMap.getSequenceStream();
    }

    @Override
    public IntStream getParallelSememeSequenceStream() {
        return sememeSequenceMap.getSequenceStream().parallel();
    }

    @Override
    public ConceptSequenceSet getConceptSequencesForNids(int[] conceptNidArray) {
        ConceptSequenceSet sequences = new ConceptSequenceSet();
        IntStream.of(conceptNidArray).forEach((nid) -> sequences.add(conceptSequenceMap.getSequenceFast(nid)));
        return sequences;
    }

    @Override
    public SememeSequenceSet getSememeSequencesForNids(int[] sememeNidArray) {
        SememeSequenceSet sequences = new SememeSequenceSet();
        IntStream.of(sememeNidArray).forEach((nid) -> sequences.add(sememeSequenceMap.getSequenceFast(nid)));
        return sequences;
    }

    @Override
    public IntStream getConceptNidsForSequences(IntStream conceptSequences) {
        return conceptSequences.map((sequence) -> {
            return getConceptNid(sequence);
        });
    }

    @Override
    public IntStream getSememeNidsForSequences(IntStream sememSequences) {
        return sememSequences.map((sequence) -> {
            return getSememeNid(sequence);
        });
    }

    @Override
    public int getRefexSequence(int nid) {
        if (nid >= 0) {
            return nid;
        }
        return refexSequenceMap.addNidIfMissing(nid);
    }

    @Override
    public int getRefexNid(int refexSequence) {
        if (refexSequence < 0) {
            return refexSequence;
        }
        return sememeSequenceMap.getNidFast(refexSequence);
    }

    @Override
    public IntStream getRefexSequenceStream() {
        return refexSequenceMap.getSequenceStream();
    }

    @Override
    public IntStream getParallelRefexSequenceStream() {
        return refexSequenceMap.getSequenceStream().parallel();
    }

    @Override
    public IntStream getRefexNidsForSequences(IntStream refexSequences) {
        return refexSequences.map((sequence) -> {
            return getRefexNid(sequence);
        });
    }

    @Override
    public RefexSequenceSet getRefexSequencesForNids(int[] refexNidArray) {
        RefexSequenceSet sequences = new RefexSequenceSet();
        IntStream.of(refexNidArray).forEach((nid) -> sequences.add(refexSequenceMap.getSequenceFast(nid)));
        return sequences;
    }

    @Override
    public int getNidForUuids(Collection<UUID> uuids) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    /**
     * For debugging...
     */
    private static HashSet<UUID> watchSet = new HashSet<>();

    {
//        watchSet.add(UUID.fromString("0418a591-f75b-39ad-be2c-3ab849326da9"));
//        watchSet.add(UUID.fromString("4459d8cf-5a6f-3952-9458-6d64324b27b7"));
    }

    @Override
    public int getNidForUuids(UUID... uuids) {
        for (UUID uuid : uuids) {
//            if (watchSet.contains(uuid)) {
//                System.out.println("Found watch: " + Arrays.asList(uuids));
//                watchSet.remove(uuid);
//            }
            int nid = uuidIntMapMap.get(uuid);
            if (nid != Integer.MAX_VALUE) {
                return nid;
            }
        }
        int nid = uuidIntMapMap.getWithGeneration(uuids[0]);
        for (int i = 1; i < uuids.length; i++) {
            uuidIntMapMap.put(uuids[i], nid);
        }
        return nid;
    }

    @Override
    public Optional<UUID> getUuidPrimordialForNid(int nid) {
        if (nid > 0) {
            nid = getConceptNid(nid);
        }
        Optional<IdentifiedObjectLocal> optionalObj
                = getIdentifiedObjectService().getIdentifiedObject(nid);
        if (optionalObj.isPresent()) {
            return Optional.of(optionalObj.get().getPrimordialUuid());
        }
        UUID[] uuids = uuidIntMapMap.getKeysForValue(nid);
        log.warn("[1] No object for nid: " + nid + " Found uuids: " + Arrays.asList(uuids));

        if (uuids.length > 0) {
            Optional.of(uuids[0]);
        }
        return Optional.empty();
    }

    @Override
    public Optional<UUID> getUuidPrimordialFromConceptSequence(int conceptSequence) {
        return getUuidPrimordialForNid(getConceptNid(conceptSequence));
    }

    /**
     * @param nid
     * @return A list of uuids corresponding with a nid.
     */
    @Override
    public List<UUID> getUuidsForNid(int nid) {
        if (nid > 0) {
            nid = getConceptNid(nid);
        }
        Optional<IdentifiedObjectLocal> optionalObj
                = getIdentifiedObjectService().getIdentifiedObject(nid);
        if (optionalObj.isPresent()) {
            return optionalObj.get().getUUIDs();
        }

        UUID[] uuids = uuidIntMapMap.getKeysForValue(nid);
        log.warn("[3] No object for nid: " + nid + " Found uuids: " + Arrays.asList(uuids));
        return Arrays.asList(uuids);
    }

    @Override
    public boolean hasUuid(UUID... uuids) {
        return Arrays.stream(uuids).anyMatch((uuid) -> (uuidIntMapMap.containsKey(uuid)));
    }

    @Override
    public boolean hasUuid(Collection<UUID> uuids) {
        return uuids.stream().anyMatch((uuid) -> (uuidIntMapMap.containsKey(uuid)));
    }

    @Override
    public void addUuidForNid(UUID uuid, int nid) {
        uuidIntMapMap.put(uuid, nid);
    }

    @Override
    public ConceptSequenceSet getConceptSequencesForReferencedComponents(SememeSequenceSet sememeSequences) {
        ConceptSequenceSet sequences = new ConceptSequenceSet();
        getSememeService(); // make sure static is initialized. 
        sememeSequences.stream().forEach((sememeSequence) -> {
            SememeChronicle<?> chronicle = sememeProvider.getSememe(sememeSequence);
            sequences.add(getConceptSequenceForComponentNid(chronicle.getReferencedComponentNid()));
        });
        return sequences;
    }

    @Override
    public int getConceptSequenceForComponentNid(int nid) {
        if (nid < 0) {
            nid = nid - Integer.MIN_VALUE;
        }
        OptionalInt returnValue = nidCnidMap.get(nid);
        if (!returnValue.isPresent()) {
            return Integer.MAX_VALUE;
        }
        return returnValue.getAsInt();
    }
    @Override
    public void setConceptSequenceForComponentNid(int conceptSequence, int nid) {
        if (nid < 0) {
            nid = nid - Integer.MIN_VALUE;
        }
        if (conceptSequence < 0) {
            conceptSequence = conceptSequenceMap.getSequenceFast(conceptSequence);
        }
        int conceptSequenceForNid = getConceptSequenceForComponentNid(nid);
        if (conceptSequenceForNid == Integer.MAX_VALUE) {
            nidCnidMap.put(nid, conceptSequence);
        } else if (conceptSequenceForNid != conceptSequence) {
            throw new IllegalStateException("Cannot change concept sequence for nid: " + nid
                    + " from: " + conceptSequence + " to: " + conceptSequenceForNid);
        }
        
    }
    @Override
    public void resetConceptSequenceForComponentNid(int conceptSequence, int nid) {
        if (nid < 0) {
            nid = nid - Integer.MIN_VALUE;
        }
        if (conceptSequence < 0) {
            conceptSequence = conceptSequenceMap.getSequenceFast(conceptSequence);
        }
        nidCnidMap.put(nid, conceptSequence);
        
    }

    public ConcurrentSequenceIntMap getNidCnidMap() {
        return nidCnidMap;
    }   

    @Override
    public int getConceptSequenceForUuids(Collection<UUID> uuids) {
        return getConceptSequenceForUuids(uuids.toArray(new UUID[uuids.size()]));
    }

    @Override
    public int getConceptSequenceForUuids(UUID... uuids) {
        return getConceptSequence(getNidForUuids(uuids));
    }
    
    @Override
    public IntStream getComponentNidStream() {
        return nidCnidMap.getComponentNidStream();
    }
    
    @Override
    public NidSet getComponentNidsForConceptNids(ConceptSequenceSet conceptSequenceSet) {
        return nidCnidMap.getComponentNidsForConceptNids(conceptSequenceSet);
     }
    
}
