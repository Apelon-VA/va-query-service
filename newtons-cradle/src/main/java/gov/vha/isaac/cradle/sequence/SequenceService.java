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
package gov.vha.isaac.cradle.sequence;

import gov.vha.isaac.cradle.IsaacDbFolder;
import gov.vha.isaac.cradle.collections.SequenceMap;
import gov.vha.isaac.ochre.api.SequenceProvider;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import gov.vha.isaac.ochre.collections.SememeSequenceSet;
import java.io.File;
import java.io.IOException;
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
public class SequenceService implements SequenceProvider {
    private static final Logger log = LogManager.getLogger();
 
    final SequenceMap conceptSequenceMap = new SequenceMap(450000);
    final SequenceMap sememeSequenceMap = new SequenceMap(3000000);

        @PostConstruct
    private void startMe() throws IOException {
        log.info("Starting SequenceService post-construct");    
        if (!IsaacDbFolder.get().getPrimordial()) {
            log.info("Loading concept-sequence.map.");
           conceptSequenceMap.read(new File(IsaacDbFolder.get().getDbFolderPath().toFile(), "concept-sequence.map"));
            log.info("Loading sememe-sequence.map.");
            sememeSequenceMap.read(new File(IsaacDbFolder.get().getDbFolderPath().toFile(), "sememe-sequence.map"));
        }
    }

    @PreDestroy
    private void stopMe() throws IOException {
        log.info("conceptSequence: {}", conceptSequenceMap.getNextSequence());
        log.info("writing concept-sequence.map.");
        conceptSequenceMap.write(new File(IsaacDbFolder.get().getDbFolderPath().toFile(), "concept-sequence.map"));
        log.info("writing sememe-sequence.map.");
        sememeSequenceMap.write(new File(IsaacDbFolder.get().getDbFolderPath().toFile(), "sememe-sequence.map"));

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
        return conceptSequences.map((sequence)->{return getConceptNid(sequence);});
    }

    @Override
    public IntStream getSememeNidsForSequences(IntStream sememSequences) {
         return sememSequences.map((sequence)->{return getSememeNid(sequence);});
    }
}
