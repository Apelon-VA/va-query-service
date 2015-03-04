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
package gov.vha.isaac.cradle.taxonomy;

import gov.vha.isaac.cradle.IsaacDbFolder;
import gov.vha.isaac.cradle.taxonomy.graph.GraphCollector;
import gov.vha.isaac.cradle.version.StampSequenceComputer;
import gov.vha.isaac.cradle.waitfree.CasSequenceObjectMap;
import gov.vha.isaac.ochre.api.SequenceProvider;
import gov.vha.isaac.ochre.api.TaxonomyProvider;
import gov.vha.isaac.ochre.api.coordinate.TaxonomyCoordinate;
import gov.vha.isaac.ochre.api.tree.Tree;
import gov.vha.isaac.ochre.api.tree.TreeNodeVisitData;
import gov.vha.isaac.ochre.api.tree.hashtree.HashTreeBuilder;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import java.io.IOException;
import java.util.Arrays;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.IntStream;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.glassfish.hk2.runlevel.RunLevel;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.jvnet.hk2.annotations.Service;

/**
 *
 * @author kec
 */
@Service
@RunLevel(value = 1)
public class TaxonomyService implements TaxonomyProvider {
    
    private static final Logger log = LogManager.getLogger();
    
    /**
     * The {@code taxonomyMap} associates concept sequence keys with a 
     * primitive taxonomy record, which represents the destination, 
     * stamp, and taxonomy flags for parent and child concepts. 
     */
    final CasSequenceObjectMap<TaxonomyRecordPrimitive> originDestinationTaxonomyRecordMap = new CasSequenceObjectMap(new TaxonomyRecordSerializer());
    final ConcurrentSkipListSet<DestinationOriginRecord> destinationOriginRecordSet = new ConcurrentSkipListSet<>();

    private SequenceProvider sequenceProvider;

    @PostConstruct
    private void startMe() throws IOException {
        log.info("Starting TaxonomyService post-construct");    
        sequenceProvider = Hk2Looker.getService(SequenceProvider.class);
        if (!IsaacDbFolder.get().getPrimordial()) {
            log.info("Reading taxonomy.");
            originDestinationTaxonomyRecordMap.read(IsaacDbFolder.get().getDbFolderPath(), "taxonomy/", ".taxonomy.map");
        }
    }

    @PreDestroy
    private void stopMe() throws IOException {
        log.info("Writing taxonomy.");
        originDestinationTaxonomyRecordMap.write(IsaacDbFolder.get().getDbFolderPath(), "taxonomy/", ".taxonomy.map");
    }

    public ConcurrentSkipListSet<DestinationOriginRecord> getDestinationOriginRecordSet() {
        return destinationOriginRecordSet;
    }
    
    public CasSequenceObjectMap<TaxonomyRecordPrimitive> getOriginDestinationTaxonomyRecords() {
        return originDestinationTaxonomyRecordMap;
    }

    @Override
    public Tree getTaxonomyTree(TaxonomyCoordinate tc) {
        IntStream conceptSequenceStream = sequenceProvider.getParallelConceptSequenceStream();
        GraphCollector collector = new GraphCollector(originDestinationTaxonomyRecordMap, tc);
        HashTreeBuilder graphBuilder = conceptSequenceStream.collect(
                HashTreeBuilder::new,
                collector,
                collector);
        return graphBuilder.getSimpleDirectedGraphGraph();
    }

    @Override
    public boolean isChildOf(int childId, int parentId, TaxonomyCoordinate tc) {      
        childId = sequenceProvider.getConceptSequence(childId);
        parentId = sequenceProvider.getConceptSequence(parentId);

        StampSequenceComputer computer = StampSequenceComputer.getComputer(tc.getStampCoordinate());
        int flags = TaxonomyFlags.getFlagsFromTaxonomyCoordinate(tc);

        Optional<TaxonomyRecordPrimitive> record = originDestinationTaxonomyRecordMap.get(childId);
        if (record.isPresent()) {
            TaxonomyRecordUnpacked childTaxonomyRecords = new TaxonomyRecordUnpacked(record.get().getArray());
            Optional<StampTaxonomyRecords> parentStampRecordsOptional = childTaxonomyRecords.getConceptSequenceStampRecords(parentId);
            if (parentStampRecordsOptional.isPresent()) {
                StampTaxonomyRecords parentStampRecords = parentStampRecordsOptional.get();
                IntStream.Builder parentStampsIntStream = IntStream.builder();
                parentStampRecords.getStampFlagStream().forEach((stampFlag) -> {
                    if ((stampFlag & flags) == flags) {
                        parentStampsIntStream
                                .add(stampFlag & TaxonomyRecordPrimitive.STAMP_BIT_MASK);
                    }
                });

                if (computer.isLatestActive(parentStampsIntStream.build())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean isKindOf(int childId, int parentId, TaxonomyCoordinate tc) {

        childId = sequenceProvider.getConceptSequence(childId);
        parentId = sequenceProvider.getConceptSequence(parentId);

        return recursiveFindAncestor(childId, parentId, tc);
    }

        private boolean recursiveFindAncestor(int childSequence, int parentSequence, 
                TaxonomyCoordinate tc) {
        // currently unpacking from array to object.
        // TODO operate directly on array if unpacking is a performance bottleneck.

        Optional<TaxonomyRecordPrimitive> record = originDestinationTaxonomyRecordMap.get(childSequence);
        if (record.isPresent()) {
            TaxonomyRecordUnpacked childTaxonomyRecords = new TaxonomyRecordUnpacked(record.get().getArray());
            int[] activeConceptSequences
                    = childTaxonomyRecords.getActiveConceptSequences(tc).toArray();
            if (Arrays.stream(activeConceptSequences).anyMatch((int activeParentSequence) -> activeParentSequence == parentSequence)) {
                return true;
            }
            return Arrays.stream(activeConceptSequences).anyMatch(
                    (int intermediateChild) -> recursiveFindAncestor(intermediateChild, parentSequence, tc));
        }
        return false;
    }

    @Override
    public ConceptSequenceSet getKindOfSequenceSet(int rootId, TaxonomyCoordinate tc) {
        rootId = sequenceProvider.getConceptSequence(rootId);
        // TODO Look at performance of getTaxonomyTree...
        Tree tree = getTaxonomyTree(tc);
        ConceptSequenceSet kindOfSet = ConceptSequenceSet.of(rootId);
        tree.depthFirstProcess(rootId, (TreeNodeVisitData t, int conceptSequence) -> {
            kindOfSet.add(conceptSequence);
        });
        return kindOfSet;
    }
    
    private enum AllowedStatus {
        ACTIVE_ONLY, ACTIVE_AND_INACTIVE;
    }
    private enum AllowedRelTypes {
        HIERARCHICAL_ONLY, ALL_RELS;
    }


    private IntStream filterOriginSequences(IntStream origins, int parentId, TaxonomyCoordinate tc, AllowedStatus allowedStatus, AllowedRelTypes allowedRelTypes) {
        return origins.filter((originSequence) -> {
            Optional<TaxonomyRecordPrimitive> taxonomyRecordOptional = originDestinationTaxonomyRecordMap.get(originSequence);
            if (taxonomyRecordOptional.isPresent()) {
                TaxonomyRecordPrimitive taxonomyRecord = taxonomyRecordOptional.get();
                if (allowedStatus == AllowedStatus.ACTIVE_ONLY) {
                    if (allowedRelTypes == AllowedRelTypes.ALL_RELS) {
                        return taxonomyRecord.containsActiveSequence(parentId, tc, TaxonomyFlags.ALL_RELS);
                    }
                    return taxonomyRecord.containsActiveSequence(parentId, tc);
                } else {
                    if (allowedRelTypes == AllowedRelTypes.ALL_RELS) {
                        return taxonomyRecord.containsVisibleSequence(parentId, tc, TaxonomyFlags.ALL_RELS);
                    }
                    return taxonomyRecord.containsVisibleSequence(parentId, tc);
                }
            }
            return false;
        });
    }
    private IntStream filterOriginSequences(IntStream origins, int parentId, int flags) {
        return origins.filter((originSequence) -> {
            Optional<TaxonomyRecordPrimitive> taxonomyRecordOptional = originDestinationTaxonomyRecordMap.get(originSequence);
            if (taxonomyRecordOptional.isPresent()) {
                TaxonomyRecordPrimitive taxonomyRecord = taxonomyRecordOptional.get();
                return taxonomyRecord.containsSequenceWithFlags(parentId, flags);
            }
            return false;
        });
    }

    private IntStream getOriginSequenceStream(int parentId) {
        // Set of all concept sequences that point to the parent. 
        NavigableSet<DestinationOriginRecord> subSet = destinationOriginRecordSet.subSet(
                new DestinationOriginRecord(parentId, Integer.MIN_VALUE), 
                new DestinationOriginRecord(parentId, Integer.MAX_VALUE));
        return subSet.stream().mapToInt((DestinationOriginRecord record) -> record.getOriginSequence());
    }



    @Override
    public int[] getTaxonomyParentSequencesActive(int childId, TaxonomyCoordinate tc) {
        childId = sequenceProvider.getConceptSequence(childId);
        Optional<TaxonomyRecordPrimitive> record = originDestinationTaxonomyRecordMap.get(childId);
        if (record.isPresent()) {
            TaxonomyRecordPrimitive trp = record.get();
            return trp.getActiveParents(tc).toArray();
        }
        return new int[0];
    }
        
    @Override
    public int[] getTaxonomyParentSequencesVisible(int childId, TaxonomyCoordinate tc) {
        childId = sequenceProvider.getConceptSequence(childId);
        Optional<TaxonomyRecordPrimitive> taxonomyRecordOptional = originDestinationTaxonomyRecordMap.get(childId);
        if (taxonomyRecordOptional.isPresent()) {
            TaxonomyRecordPrimitive taxonomyRecord = taxonomyRecordOptional.get();
            return taxonomyRecord.getVisibleParents(tc).toArray();
        }
        return new int[0];
    }

    @Override
    public int[] getTaxonomyParentSequences(int childId) {
        childId = sequenceProvider.getConceptSequence(childId);
        Optional<TaxonomyRecordPrimitive> taxonomyRecordOptional = originDestinationTaxonomyRecordMap.get(childId);
        if (taxonomyRecordOptional.isPresent()) {
            TaxonomyRecordPrimitive taxonomyRecord = taxonomyRecordOptional.get();
            return taxonomyRecord.getParents().distinct().toArray();
        }
        return new int[0];
    }

    @Override
    public int[] getRoots(TaxonomyCoordinate tc) {
        Tree tree = getTaxonomyTree(tc);
        return tree.getRootSequences();
    }
    
    @Override
    public int[] getTaxonomyChildSequencesActive(int parentId, TaxonomyCoordinate tc) {
        parentId = sequenceProvider.getConceptSequence(parentId);
        IntStream origins = getOriginSequenceStream(parentId);
        
        return filterOriginSequences(origins, parentId, tc, AllowedStatus.ACTIVE_ONLY, AllowedRelTypes.HIERARCHICAL_ONLY).toArray();
    }

    @Override
    public int[] getTaxonomyChildSequencesVisible(int parentId, TaxonomyCoordinate tc) {
        parentId = sequenceProvider.getConceptSequence(parentId);
        // Set of all concept sequences that point to the parent. 
        IntStream origins = getOriginSequenceStream(parentId);
        return filterOriginSequences(origins, parentId, tc, AllowedStatus.ACTIVE_AND_INACTIVE, AllowedRelTypes.HIERARCHICAL_ONLY).toArray();
    }

    @Override
    public int[] getTaxonomyChildSequences(int parentId) {
        parentId = sequenceProvider.getConceptSequence(parentId);
        // Set of all concept sequences that point to the parent. 
        IntStream origins = getOriginSequenceStream(parentId);
        return filterOriginSequences(origins, parentId, TaxonomyFlags.PARENT_FLAG_SET).toArray();
    }

    @Override
    public int[] getAllRelationshipOriginSequencesActive(int destination, TaxonomyCoordinate tc) {
        destination = sequenceProvider.getConceptSequence(destination);
        // Set of all concept sequences that point to the parent. 
        IntStream origins = getOriginSequenceStream(destination);
        return filterOriginSequences(origins, destination, tc,  AllowedStatus.ACTIVE_ONLY, AllowedRelTypes.ALL_RELS).toArray();
    }

    @Override
    public int[] getAllRelationshipOriginSequencesVisible(int destination, TaxonomyCoordinate tc) {
        destination = sequenceProvider.getConceptSequence(destination);
        // Set of all concept sequences that point to the parent. 
        IntStream origins = getOriginSequenceStream(destination);
        return filterOriginSequences(origins, destination, tc, AllowedStatus.ACTIVE_AND_INACTIVE, AllowedRelTypes.ALL_RELS).toArray();
    }

    
    @Override
    public int[] getAllRelationshipOriginSequences(int destination) {
        destination = sequenceProvider.getConceptSequence(destination);
        return getOriginSequenceStream(destination).toArray();
    }

}
