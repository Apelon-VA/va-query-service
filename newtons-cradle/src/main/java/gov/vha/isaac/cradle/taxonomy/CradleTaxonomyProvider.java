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

import gov.vha.isaac.cradle.Cradle;
import gov.vha.isaac.cradle.builders.ConceptActiveService;
import gov.vha.isaac.cradle.taxonomy.graph.GraphCollector;
import gov.vha.isaac.cradle.waitfree.CasSequenceObjectMap;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.IdentifierService;
import gov.vha.isaac.ochre.api.SystemStatusService;
import gov.vha.isaac.ochre.api.TaxonomyService;
import gov.vha.isaac.ochre.api.TaxonomySnapshotService;
import gov.vha.isaac.ochre.api.coordinate.StampCoordinate;
import gov.vha.isaac.ochre.api.coordinate.TaxonomyCoordinate;
import gov.vha.isaac.ochre.api.snapshot.calculator.RelativePositionCalculator;
import gov.vha.isaac.ochre.api.tree.Tree;
import gov.vha.isaac.ochre.api.tree.TreeNodeVisitData;
import gov.vha.isaac.ochre.api.tree.hashtree.HashTreeBuilder;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
public class CradleTaxonomyProvider implements TaxonomyService, ConceptActiveService {

    private static final Logger log = LogManager.getLogger();

    /**
     * The {@code taxonomyMap} associates concept sequence keys with a primitive
     * taxonomy record, which represents the destination, stamp, and taxonomy
     * flags for parent and child concepts.
     */
    final CasSequenceObjectMap<TaxonomyRecordPrimitive> originDestinationTaxonomyRecordMap
            = new CasSequenceObjectMap(new TaxonomyRecordSerializer(),
                    Cradle.getCradlePath().resolve(TAXONOMY), "seg.", ".taxonomy.map");
    private static final String TAXONOMY = "taxonomy";
    final ConcurrentSkipListSet<DestinationOriginRecord> destinationOriginRecordSet = new ConcurrentSkipListSet<>();

    private IdentifierService sequenceProvider;

    private CradleTaxonomyProvider() {
        log.info("CradleTaxonomyProvider constructed");
    }

    @PostConstruct
    private void startMe() throws IOException {
        try {
            log.info("Starting TaxonomyService post-construct");
            sequenceProvider = Hk2Looker.getService(IdentifierService.class);
            if (!Cradle.cradleStartedEmpty()) {
                log.info("Reading taxonomy.");
                originDestinationTaxonomyRecordMap.initialize();
                File inputFile = new File(Cradle.getCradlePath().resolve(TAXONOMY).toFile(), ORIGIN_DESTINATION_MAP);
                try (DataInputStream in = new DataInputStream(new BufferedInputStream(
                        new FileInputStream(inputFile)))) {
                    int size = in.readInt(); 
                    for (int i = 0; i < size; i++) {
                        destinationOriginRecordSet.add(new DestinationOriginRecord(in.readInt(), in.readInt()));
                    }
                }
            }

        } catch (Exception e) {
            LookupService.getService(SystemStatusService.class).notifyServiceConfigurationFailure("Cradle Taxonomy Provider", e);
            throw e;
        }
    }

    @PreDestroy
    private void stopMe() throws IOException {
        log.info("Writing taxonomy.");
        originDestinationTaxonomyRecordMap.write();
        File outputFile = new File(Cradle.getCradlePath().resolve(TAXONOMY).toFile(), ORIGIN_DESTINATION_MAP);
        outputFile.getParentFile().mkdirs();
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
                new FileOutputStream(outputFile)))) {
            out.writeInt(destinationOriginRecordSet.size());
            destinationOriginRecordSet.forEach((rec) -> {
                try {
                    out.writeInt(rec.getDestinationSequence());
                    out.writeInt(rec.getOriginSequence());
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }

            });
        }
    }
    private static final String ORIGIN_DESTINATION_MAP = "origin-destination.map";

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

        RelativePositionCalculator computer = RelativePositionCalculator.getCalculator(tc.getStampCoordinate());
        int flags = TaxonomyFlags.getFlagsFromTaxonomyCoordinate(tc);

        Optional<TaxonomyRecordPrimitive> record = originDestinationTaxonomyRecordMap.get(childId);
        if (record.isPresent()) {
            TaxonomyRecordUnpacked childTaxonomyRecords = new TaxonomyRecordUnpacked(record.get().getArray());
            Optional<TypeStampTaxonomyRecords> parentStampRecordsOptional = childTaxonomyRecords.getConceptSequenceStampRecords(parentId);
            if (parentStampRecordsOptional.isPresent()) {
                TypeStampTaxonomyRecords parentStampRecords = parentStampRecordsOptional.get();
                if (computer.isLatestActive(parentStampRecords.getStampsOfTypeWithFlags(
                        IsaacMetadataAuxiliaryBinding.IS_A.getSequence(), flags))) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean wasEverKindOf(int childId, int parentId) {
        childId = sequenceProvider.getConceptSequence(childId);
        parentId = sequenceProvider.getConceptSequence(parentId);
        if (childId == parentId) {
            return true;
        }
        return recursiveFindAncestor(childId, parentId);
    }

    private boolean recursiveFindAncestor(int childSequence, int parentSequence) {
        // currently unpacking from array to object. 
        // TODO operate directly on array if unpacking is a performance bottleneck.

        Optional<TaxonomyRecordPrimitive> record = originDestinationTaxonomyRecordMap.get(childSequence);
        if (record.isPresent()) {
            TaxonomyRecordUnpacked childTaxonomyRecords = new TaxonomyRecordUnpacked(record.get().getArray());
            int[] conceptSequencesForType
                    = childTaxonomyRecords.getConceptSequencesForType(
                            IsaacMetadataAuxiliaryBinding.IS_A.getSequence()).toArray();
            if (Arrays.stream(conceptSequencesForType).anyMatch((int parentSequenceOfType) -> parentSequenceOfType == parentSequence)) {
                return true;
            }
            return Arrays.stream(conceptSequencesForType).anyMatch(
                    (int intermediateChild) -> recursiveFindAncestor(intermediateChild, parentSequence));
        }
        return false;
    }
    

    @Override
    public boolean isKindOf(int childId, int parentId, TaxonomyCoordinate tc) {

        childId = sequenceProvider.getConceptSequence(childId);
        parentId = sequenceProvider.getConceptSequence(parentId);
        if (childId == parentId) {
            return true;
        }
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
                    = childTaxonomyRecords.getActiveConceptSequencesForType(
                            IsaacMetadataAuxiliaryBinding.IS_A.getSequence(), tc).toArray();
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

    @Override
    public boolean isConceptActive(int conceptSequence, StampCoordinate stampCoordinate) {
        Optional<TaxonomyRecordPrimitive> taxonomyRecordOptional
                = originDestinationTaxonomyRecordMap.get(conceptSequence);
        if (taxonomyRecordOptional.isPresent()) {
            return taxonomyRecordOptional.get().isConceptActive(conceptSequence, stampCoordinate);
        }
        return false;
    }

    private enum AllowedStatus {

        ACTIVE_ONLY, ACTIVE_AND_INACTIVE;
    }

    private enum AllowedRelTypes {

        HIERARCHICAL_ONLY, ALL_RELS;
    }
    
    private IntStream filterOriginSequences(IntStream origins, int parentSequence, ConceptSequenceSet typeSequenceSet) {
        return origins.filter((originSequence) -> {
            Optional<TaxonomyRecordPrimitive> taxonomyRecordOptional = originDestinationTaxonomyRecordMap.get(originSequence);
            if (taxonomyRecordOptional.isPresent()) {
                TaxonomyRecordPrimitive taxonomyRecord = taxonomyRecordOptional.get();
                return taxonomyRecord.containsSequenceViaType(parentSequence, typeSequenceSet, TaxonomyFlags.ALL_RELS);
            }
            return false;
        });
    }

    private IntStream filterOriginSequences(IntStream origins, int parentSequence, ConceptSequenceSet typeSequenceSet, TaxonomyCoordinate tc, AllowedStatus allowedStatus) {
        return origins.filter((originSequence) -> {
            Optional<TaxonomyRecordPrimitive> taxonomyRecordOptional = originDestinationTaxonomyRecordMap.get(originSequence);
            if (taxonomyRecordOptional.isPresent()) {
                TaxonomyRecordPrimitive taxonomyRecord = taxonomyRecordOptional.get();
                if (allowedStatus == AllowedStatus.ACTIVE_ONLY) {
                    return taxonomyRecord.containsActiveSequenceViaType(parentSequence, typeSequenceSet, tc, TaxonomyFlags.ALL_RELS);
                } else {
                    return taxonomyRecord.containsVisibleSequenceViaType(parentSequence, typeSequenceSet, tc, TaxonomyFlags.ALL_RELS);
                }
            }
            return false;
        });
    }
    private IntStream filterOriginSequences(IntStream origins, int parentSequence, int typeSequence, TaxonomyCoordinate tc, AllowedStatus allowedStatus, AllowedRelTypes allowedRelTypes) {
        return origins.filter((originSequence) -> {
            Optional<TaxonomyRecordPrimitive> taxonomyRecordOptional = originDestinationTaxonomyRecordMap.get(originSequence);
            if (taxonomyRecordOptional.isPresent()) {
                TaxonomyRecordPrimitive taxonomyRecord = taxonomyRecordOptional.get();
                if (allowedStatus == AllowedStatus.ACTIVE_ONLY) {
                    if (allowedRelTypes == AllowedRelTypes.ALL_RELS) {
                        return taxonomyRecord.containsActiveSequenceViaType(parentSequence, typeSequence, tc, TaxonomyFlags.ALL_RELS);
                    }
                    return taxonomyRecord.containsActiveSequenceViaType(parentSequence, typeSequence, tc);
                } else {
                    if (allowedRelTypes == AllowedRelTypes.ALL_RELS) {
                        return taxonomyRecord.containsVisibleSequenceViaType(parentSequence, typeSequence, tc, TaxonomyFlags.ALL_RELS);
                    }
                    return taxonomyRecord.containsVisibleSequenceViaType(parentSequence, typeSequence, tc);
                }
            }
            return false;
        });
    }

    private IntStream filterOriginSequences(IntStream origins, int parentSequence, int typeSequence, int flags) {
        return origins.filter((originSequence) -> {
            Optional<TaxonomyRecordPrimitive> taxonomyRecordOptional = originDestinationTaxonomyRecordMap.get(originSequence);
            if (taxonomyRecordOptional.isPresent()) {
                TaxonomyRecordPrimitive taxonomyRecord = taxonomyRecordOptional.get();
                return taxonomyRecord.containsSequenceViaTypeWithFlags(parentSequence, typeSequence, flags);
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
    public IntStream getTaxonomyParentSequencesActive(int childId, TaxonomyCoordinate tc) {
        childId = sequenceProvider.getConceptSequence(childId);
        Optional<TaxonomyRecordPrimitive> record = originDestinationTaxonomyRecordMap.get(childId);
        if (record.isPresent()) {
            TaxonomyRecordPrimitive trp = record.get();
            return trp.getActiveParentSequences(tc);
        }
        return IntStream.empty();
    }

    @Override
    public IntStream getTaxonomyParentSequencesVisible(int childId, TaxonomyCoordinate tc) {
        childId = sequenceProvider.getConceptSequence(childId);
        Optional<TaxonomyRecordPrimitive> taxonomyRecordOptional = originDestinationTaxonomyRecordMap.get(childId);
        if (taxonomyRecordOptional.isPresent()) {
            TaxonomyRecordPrimitive taxonomyRecord = taxonomyRecordOptional.get();
            return taxonomyRecord.getVisibleParentSequences(tc);
        }
        return IntStream.empty();
    }

    @Override
    public IntStream getTaxonomyParentSequences(int childId) {
        childId = sequenceProvider.getConceptSequence(childId);
        Optional<TaxonomyRecordPrimitive> taxonomyRecordOptional = originDestinationTaxonomyRecordMap.get(childId);
        if (taxonomyRecordOptional.isPresent()) {
            TaxonomyRecordPrimitive taxonomyRecord = taxonomyRecordOptional.get();
            return taxonomyRecord.getParentSequences().distinct();
        }
        return IntStream.empty();
    }

    @Override
    public IntStream getRoots(TaxonomyCoordinate tc) {
        Tree tree = getTaxonomyTree(tc);
        return tree.getRootSequenceStream();
    }

    @Override
    public IntStream getTaxonomyChildSequencesActive(int parentId, TaxonomyCoordinate tc) {
        parentId = sequenceProvider.getConceptSequence(parentId);
        IntStream origins = getOriginSequenceStream(parentId);

        return filterOriginSequences(origins, parentId,
                IsaacMetadataAuxiliaryBinding.IS_A.getSequence(), tc,
                AllowedStatus.ACTIVE_ONLY, AllowedRelTypes.HIERARCHICAL_ONLY);
    }

    @Override
    public IntStream getTaxonomyChildSequencesVisible(int parentId, TaxonomyCoordinate tc) {
        parentId = sequenceProvider.getConceptSequence(parentId);
        // Set of all concept sequences that point to the parent. 
        IntStream origins = getOriginSequenceStream(parentId);
        return filterOriginSequences(origins, parentId,
                IsaacMetadataAuxiliaryBinding.IS_A.getSequence(), tc,
                AllowedStatus.ACTIVE_AND_INACTIVE, AllowedRelTypes.HIERARCHICAL_ONLY);
    }

    @Override
    public IntStream getTaxonomyChildSequences(int parentId) {
        parentId = sequenceProvider.getConceptSequence(parentId);
        // Set of all concept sequences that point to the parent. 
        IntStream origins = getOriginSequenceStream(parentId);
        return filterOriginSequences(origins, parentId,
                IsaacMetadataAuxiliaryBinding.IS_A.getSequence(), TaxonomyFlags.ALL_RELS);
    }

    @Override
    public IntStream getAllRelationshipOriginSequencesActive(int destination, TaxonomyCoordinate tc) {
        destination = sequenceProvider.getConceptSequence(destination);
        // Set of all concept sequences that point to the parent. 
        IntStream origins = getOriginSequenceStream(destination);
        return filterOriginSequences(origins, destination,
                IsaacMetadataAuxiliaryBinding.IS_A.getSequence(), tc,
                AllowedStatus.ACTIVE_ONLY, AllowedRelTypes.ALL_RELS);
    }

    @Override
    public IntStream getAllRelationshipOriginSequencesVisible(int destination, TaxonomyCoordinate tc) {
        destination = sequenceProvider.getConceptSequence(destination);
        // Set of all concept sequences that point to the parent. 
        IntStream origins = getOriginSequenceStream(destination);
        return filterOriginSequences(origins, destination,
                IsaacMetadataAuxiliaryBinding.IS_A.getSequence(), tc,
                AllowedStatus.ACTIVE_AND_INACTIVE, AllowedRelTypes.ALL_RELS);
    }

    @Override
    public IntStream getAllRelationshipOriginSequences(int destination) {
        destination = sequenceProvider.getConceptSequence(destination);
        return getOriginSequenceStream(destination);
    }

    @Override
    public ConceptSequenceSet getChildOfSequenceSet(int parentId, TaxonomyCoordinate tc) {
        parentId = sequenceProvider.getConceptSequence(parentId);
        // Set of all concept sequences that point to the parent. 
        IntStream origins = getOriginSequenceStream(parentId);
        return ConceptSequenceSet.of(filterOriginSequences(origins, parentId,
                IsaacMetadataAuxiliaryBinding.IS_A.getSequence(), tc,
                AllowedStatus.ACTIVE_ONLY, AllowedRelTypes.HIERARCHICAL_ONLY));
    }

    @Override
    public IntStream getAllRelationshipDestinationSequencesActive(int originId, TaxonomyCoordinate tc) {
        return getAllRelationshipDestinationSequencesOfTypeActive(originId, new ConceptSequenceSet(), tc);
    }

    @Override
    public IntStream getAllRelationshipDestinationSequencesVisible(int originId, TaxonomyCoordinate tc) {
        return getAllRelationshipDestinationSequencesOfTypeVisible(originId, new ConceptSequenceSet(), tc);
    }

    @Override
    public IntStream getAllRelationshipDestinationSequences(int originId) {
        originId = sequenceProvider.getConceptSequence(originId);
        Optional<TaxonomyRecordPrimitive> taxonomyRecordOptional = originDestinationTaxonomyRecordMap.get(originId);
        if (taxonomyRecordOptional.isPresent()) {
            return taxonomyRecordOptional.get().getDestinationSequences();
        }
        return IntStream.empty();
    }

    @Override
    public IntStream getAllRelationshipDestinationSequencesOfTypeActive(int originId, ConceptSequenceSet typeSequenceSet, TaxonomyCoordinate tc) {
        originId = sequenceProvider.getConceptSequence(originId);
        Optional<TaxonomyRecordPrimitive> taxonomyRecordOptional = originDestinationTaxonomyRecordMap.get(originId);
        if (taxonomyRecordOptional.isPresent()) {
            return taxonomyRecordOptional.get().getDestinationSequencesOfTypeActive(typeSequenceSet, tc);
        }
        return IntStream.empty();
    }

    @Override
    public IntStream getAllRelationshipDestinationSequencesOfTypeVisible(int originId, ConceptSequenceSet typeSequenceSet, TaxonomyCoordinate tc) {
        originId = sequenceProvider.getConceptSequence(originId);
        Optional<TaxonomyRecordPrimitive> taxonomyRecordOptional = originDestinationTaxonomyRecordMap.get(originId);
        if (taxonomyRecordOptional.isPresent()) {
            return taxonomyRecordOptional.get().getDestinationSequencesOfTypeVisible(typeSequenceSet, tc);
        }
        return IntStream.empty();
    }

    @Override
    public IntStream getAllRelationshipDestinationSequencesOfType(int originId, ConceptSequenceSet typeSequenceSet) {
        originId = sequenceProvider.getConceptSequence(originId);
        Optional<TaxonomyRecordPrimitive> taxonomyRecordOptional = originDestinationTaxonomyRecordMap.get(originId);
        if (taxonomyRecordOptional.isPresent()) {
            return taxonomyRecordOptional.get().getDestinationSequencesOfType(typeSequenceSet);
        }
        return IntStream.empty();
    }

    @Override
    public IntStream getAllRelationshipOriginSequencesOfTypeActive(int destinationId, ConceptSequenceSet typeSequenceSet, TaxonomyCoordinate tc) {
        destinationId = sequenceProvider.getConceptSequence(destinationId);
        // Set of all concept sequences that point to the parent. 
        IntStream origins = getOriginSequenceStream(destinationId);
        return filterOriginSequences(origins, destinationId,
                typeSequenceSet, tc,
                AllowedStatus.ACTIVE_ONLY);
   }

    @Override
    public IntStream getAllRelationshipOriginSequencesOfTypeVisible(int destinationId, ConceptSequenceSet typeSequenceSet, TaxonomyCoordinate tc) {
        destinationId = sequenceProvider.getConceptSequence(destinationId);
        // Set of all concept sequences that point to the parent. 
        IntStream origins = getOriginSequenceStream(destinationId);
        return filterOriginSequences(origins, destinationId,
                typeSequenceSet, tc,
                AllowedStatus.ACTIVE_AND_INACTIVE);
    }

    @Override
    public IntStream getAllRelationshipOriginSequencesOfType(int destinationId, ConceptSequenceSet typeSequenceSet) {
        destinationId = sequenceProvider.getConceptSequence(destinationId);
        // Set of all concept sequences that point to the parent. 
        IntStream origins = getOriginSequenceStream(destinationId);
        return filterOriginSequences(origins, destinationId,
                typeSequenceSet);
     }

    @Override
    public TaxonomySnapshotService getSnapshot(TaxonomyCoordinate tc) {
        return new TaxonomySnapshotProvider(tc);
    }

    private class TaxonomySnapshotProvider implements TaxonomySnapshotService {

        TaxonomyCoordinate tc;

        public TaxonomySnapshotProvider(TaxonomyCoordinate tc) {
            this.tc = tc;
        }

        @Override
        public Tree getTaxonomyTree() {
            return CradleTaxonomyProvider.this.getTaxonomyTree(tc);
        }

        @Override
        public boolean isChildOf(int childId, int parentId) {
            return CradleTaxonomyProvider.this.isChildOf(childId, parentId, tc);
        }

        @Override
        public boolean isKindOf(int childId, int parentId) {
            return CradleTaxonomyProvider.this.isKindOf(childId, parentId, tc);
        }

        @Override
        public ConceptSequenceSet getKindOfSequenceSet(int rootId) {
            return CradleTaxonomyProvider.this.getKindOfSequenceSet(rootId, tc);
        }

        @Override
        public IntStream getAllRelationshipOriginSequencesActive(int destination) {
            return CradleTaxonomyProvider.this.getAllRelationshipOriginSequencesActive(destination, tc);
        }

        @Override
        public IntStream getTaxonomyChildSequencesActive(int parentId) {
            return CradleTaxonomyProvider.this.getTaxonomyChildSequencesActive(parentId, tc);
        }

        @Override
        public IntStream getTaxonomyParentSequencesActive(int childId) {
            return CradleTaxonomyProvider.this.getTaxonomyParentSequencesActive(childId, tc);
        }

        @Override
        public IntStream getRoots() {
            return CradleTaxonomyProvider.this.getRoots(tc);
        }

        @Override
        public IntStream getAllRelationshipDestinationSequencesOfTypeActive(int originId, ConceptSequenceSet typeSequenceSet) {
            return CradleTaxonomyProvider.this.getAllRelationshipDestinationSequencesOfTypeActive(originId, typeSequenceSet, tc);
        }

        @Override
        public IntStream getAllRelationshipDestinationSequencesOfTypeVisible(int originId, ConceptSequenceSet typeSequenceSet) {
           return CradleTaxonomyProvider.this.getAllRelationshipDestinationSequencesOfTypeVisible(originId, typeSequenceSet, tc);
        }

        @Override
        public IntStream getAllRelationshipOriginSequencesOfTypeActive(int destinationId, ConceptSequenceSet typeSequenceSet) {
            return CradleTaxonomyProvider.this.getAllRelationshipOriginSequencesOfTypeActive(destinationId, typeSequenceSet, tc);
        }

        @Override
        public IntStream getAllRelationshipOriginSequencesOfTypeVisible(int destinationId, ConceptSequenceSet typeSequenceSet) {
            return CradleTaxonomyProvider.this.getAllRelationshipOriginSequencesOfTypeVisible(destinationId, typeSequenceSet, tc);
        }
    }
}
