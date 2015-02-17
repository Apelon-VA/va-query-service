package gov.vha.isaac.cradle.tasks;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.cradle.collections.CasSequenceObjectMap;
import gov.vha.isaac.cradle.component.ConceptChronicleDataEager;
import gov.vha.isaac.cradle.taxonomy.PrimitiveTaxonomyRecord;
import gov.vha.isaac.cradle.taxonomy.TaxonomyFlags;
import java.io.IOException;
import java.util.EnumSet;

import gov.vha.isaac.ochre.api.ConceptProxy;
import org.ihtsdo.otf.tcc.dto.TtkConceptChronicle;
import org.ihtsdo.otf.tcc.dto.component.TtkRevision;
import org.ihtsdo.otf.tcc.dto.component.TtkRevisionProcessorBI;
import org.ihtsdo.otf.tcc.model.cc.concept.ConceptChronicle;

import java.util.UUID;
import java.util.concurrent.*;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;
import org.ihtsdo.otf.tcc.api.metadata.binding.SnomedMetadataRf2;
import org.ihtsdo.otf.tcc.api.metadata.binding.TermAux;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.ihtsdo.otf.tcc.model.cc.relationship.Relationship;
import org.ihtsdo.otf.tcc.model.cc.relationship.RelationshipVersion;

/**
 * Created by kec on 7/20/14.
 */
public class ImportEConcept implements Callable<Void> {
    
    private static final int snomedIsaNid;
    private static final int auxIsaNid;

    private static final int snomedStatedNid;
    private static final int auxStatedNid;
    private static final int snomedInferredNid;
    private static final int auxInferredNid;
    private static final CradleExtensions cradle;
    public static CasSequenceObjectMap<PrimitiveTaxonomyRecord> taxonomyRecords;

    static {
        try {
            cradle = Hk2Looker.get().getService(CradleExtensions.class);
            taxonomyRecords = cradle.getTaxonomyMap();
            snomedIsaNid = cradle.getNidForUuids(Snomed.IS_A.getUuids());
            auxIsaNid = cradle.getNidForUuids(TermAux.IS_A.getUuids());
            snomedStatedNid = cradle.getNidForUuids(SnomedMetadataRf2.STATED_RELATIONSHIP_RF2.getUuids());
            auxStatedNid = cradle.getNidForUuids(TermAux.REL_STATED_CHAR.getUuids());
            snomedInferredNid = cradle.getNidForUuids(SnomedMetadataRf2.INFERRED_RELATIONSHIP_RF2.getUuids());
            auxInferredNid = cradle.getNidForUuids(TermAux.REL_INFERED_CHAR.getUuids());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    TtkConceptChronicle eConcept;
    Semaphore permit;
    UUID newPathUuid = null;

    public ImportEConcept(TtkConceptChronicle eConcept,
                          Semaphore permit, UUID newPathUuid) {
        this(eConcept, permit);
        this.newPathUuid = newPathUuid;
    }

    public ImportEConcept(TtkConceptChronicle eConcept,
                          Semaphore permit) {
        this.eConcept = eConcept;
        this.permit = permit;
    }

    @Override
    public Void call() throws Exception {
        try {
            if (this.newPathUuid != null) {
                eConcept.processComponentRevisions(r -> r.setPathUuid(newPathUuid));
            }

            int conceptNid = cradle.getNidForUuids(eConcept.getPrimordialUuid());

            ConceptChronicle cc = ConceptChronicle.get(conceptNid);
            ConceptChronicle.mergeWithEConcept(eConcept, cc);
            ConceptChronicleDataEager conceptData = (ConceptChronicleDataEager) cc.getData();
            int conceptSequence = cradle.getConceptSequence(cc.getNid());
            
                PrimitiveTaxonomyRecord parentTaxonomyRecord;
                if (taxonomyRecords.containsKey(conceptSequence)) {
                    parentTaxonomyRecord = taxonomyRecords.get(conceptSequence).get();
                } else {
                    parentTaxonomyRecord = new PrimitiveTaxonomyRecord();
                }

                for (Relationship rel : conceptData.getSourceRels()) {
                    int destinationSequence
                            = cradle.getConceptSequence(rel.getDestinationNid());
                    assert destinationSequence != conceptSequence;
                    PrimitiveTaxonomyRecord childTaxonomyRecord;
                    if (taxonomyRecords.containsKey(destinationSequence)) {
                        childTaxonomyRecord = taxonomyRecords.get(destinationSequence).get();
                    } else {
                        childTaxonomyRecord = new PrimitiveTaxonomyRecord();
                    }
                    
                    rel.getVersions().stream().forEach((rv) -> {
                        EnumSet parentRecordFlags = EnumSet.of(TaxonomyFlags.PARENT);
                        EnumSet childRecordFlags = EnumSet.of(TaxonomyFlags.CHILD);
                        if (isaRelType(rv)) {
                            if (statedRelType(rv)) {
                                parentRecordFlags.add(TaxonomyFlags.STATED);
                                childRecordFlags.add(TaxonomyFlags.STATED);
                            } else if (inferredRelType(rv)) {
                                parentRecordFlags.add(TaxonomyFlags.INFERRED);
                                childRecordFlags.add(TaxonomyFlags.INFERRED);
                            }
                            parentTaxonomyRecord.getTaxonomyRecordUnpacked().addStampRecord(destinationSequence, rv.getStamp(), parentRecordFlags);
                            childTaxonomyRecord.getTaxonomyRecordUnpacked().addStampRecord(conceptSequence, rv.getStamp(), childRecordFlags);
                        }
                    });
                    taxonomyRecords.put(destinationSequence, childTaxonomyRecord);
                }
                taxonomyRecords.put(conceptSequence, parentTaxonomyRecord);            
            cradle.writeConceptData(conceptData);
            return null;
        } finally {
            permit.release();
        }
    }

    private static boolean inferredRelType(RelationshipVersion rv) {
        return rv.getCharacteristicNid() == snomedInferredNid
                || rv.getCharacteristicNid() == auxInferredNid;
    }

    private static boolean statedRelType(RelationshipVersion rv) {
        return rv.getCharacteristicNid() == snomedStatedNid
                || rv.getCharacteristicNid() == auxStatedNid;
    }

    private static boolean isaRelType(RelationshipVersion rv) {
        return rv.getTypeNid() == snomedIsaNid || rv.getTypeNid() == auxIsaNid;
    }


}
