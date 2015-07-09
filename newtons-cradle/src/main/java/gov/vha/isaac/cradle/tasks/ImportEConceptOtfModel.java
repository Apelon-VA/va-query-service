package gov.vha.isaac.cradle.tasks;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.cradle.waitfree.CasSequenceObjectMap;
import gov.vha.isaac.cradle.component.ConceptChronicleDataEager;
import gov.vha.isaac.cradle.taxonomy.DestinationOriginRecord;
import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordPrimitive;
import gov.vha.isaac.cradle.taxonomy.TaxonomyFlags;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.Get;
import org.ihtsdo.otf.tcc.dto.TtkConceptChronicle;
import org.ihtsdo.otf.tcc.model.cc.concept.ConceptChronicle;
import java.util.UUID;
import java.util.concurrent.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ihtsdo.otf.tcc.api.concurrency.ConcurrentReentrantLocks;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.dto.TtkConceptLock;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.ihtsdo.otf.tcc.model.cc.attributes.ConceptAttributesVersion;

/**
 * Created by kec on 7/20/14.
 */
public class ImportEConceptOtfModel implements Callable<Void> {
    private static final Logger log = LogManager.getLogger();

    private static final CradleExtensions cradle = Hk2Looker.getService(CradleExtensions.class);

    private static final int isaNid;

    private static final int statedNid;
    private static final int inferredNid;
    public static CasSequenceObjectMap<TaxonomyRecordPrimitive> originDestinationTaxonomyRecords;
    public static ConcurrentSkipListSet<DestinationOriginRecord> destinationOriginRecordSet;
    private static ConcurrentReentrantLocks conceptLockPool;

    static {
        originDestinationTaxonomyRecords = cradle.getOriginDestinationTaxonomyMap();
        destinationOriginRecordSet = cradle.getDestinationOriginRecordSet();
        isaNid = cradle.getNidForUuids(IsaacMetadataAuxiliaryBinding.IS_A.getUuids());
        statedNid = cradle.getNidForUuids(IsaacMetadataAuxiliaryBinding.STATED.getUuids());
        inferredNid = cradle.getNidForUuids(IsaacMetadataAuxiliaryBinding.INFERRED.getUuids());
        conceptLockPool = new ConcurrentReentrantLocks(128);
    }

    TtkConceptChronicle eConcept;
    UUID newPathUuid = null;
    int lastRelCharacteristic = Integer.MAX_VALUE;
    int recordFlags = Integer.MAX_VALUE;

    public ImportEConceptOtfModel(TtkConceptChronicle eConcept, UUID newPathUuid) {
        this(eConcept);
        this.newPathUuid = newPathUuid;
    }

    public ImportEConceptOtfModel(TtkConceptChronicle eConcept) {
        this.eConcept = eConcept;
    }

    @Override
    public Void call() throws Exception {

        Integer lockId = null;
        try {
            if (this.newPathUuid != null) {
                eConcept.processComponentRevisions(r -> r.setPathUuid(newPathUuid));
            }

            int conceptNid = cradle.getNidForUuids(eConcept.getPrimordialUuid());
            lockId = eConcept.getPrimordialUuid().hashCode();
            //Note that this isn't 100% correct, if one were trying to do a merge on two concepts that had different primordial UUIDs (but shared alternate IDs)
            //and were intended to be merged.  Not sure how we generated a lock ID for that case... this handles the cases I need handled at the moment... as 
            //the merge cases I have all involve concepts where the UUIDs are specified in the same order, so this lock blocks multi-thread corruption when an 
            //eConcept file lists the same concept multiple times (and requires merging)
            conceptLockPool.lock(lockId);
            
            cradle.setConceptNidForNid(conceptNid, conceptNid);
            ConceptChronicle cc = ConceptChronicle.get(conceptNid);
            ConceptChronicle.mergeWithEConcept(eConcept, cc);
            ConceptChronicleDataEager conceptData = (ConceptChronicleDataEager) cc.getData();
            int originSequence = Get.identifierService().getConceptSequence(cc.getNid());

            TaxonomyRecordPrimitive parentTaxonomyRecord;
            if (originDestinationTaxonomyRecords.containsKey(originSequence)) {
                parentTaxonomyRecord = originDestinationTaxonomyRecords.get(originSequence).get();
            } else {
                parentTaxonomyRecord = new TaxonomyRecordPrimitive();
            }

            if (conceptData.getConceptAttributes() != null) {
                for (ConceptAttributesVersion cav : conceptData.getConceptAttributes().getVersions()) {
                    parentTaxonomyRecord.getTaxonomyRecordUnpacked().addStampRecord(originSequence, 
                            originSequence, cav.getStamp(), TaxonomyFlags.CONCEPT_STATUS.bits);
                }

                processTaxonomy(conceptData, originSequence, parentTaxonomyRecord);
                originDestinationTaxonomyRecords.put(originSequence, parentTaxonomyRecord);
                cradle.writeConceptData(conceptData);
            }
            return null;
        } catch (Exception e) {
            System.err.println("Failure importing " + eConcept.toString());
            throw e;
        }
        finally {
            if (lockId != null) {
                conceptLockPool.unlock(lockId);
            }
        }
    }

    private void processTaxonomy(ConceptChronicleDataEager conceptData, 
            int originSequence, 
            TaxonomyRecordPrimitive parentTaxonomyRecord) {
        conceptData.getSourceRels().stream().map((rel) -> {
            int destinationSequence
                    = Get.identifierService().getConceptSequence(
                            rel.getDestinationNid());
            assert destinationSequence != originSequence;
            lastRelCharacteristic = Integer.MAX_VALUE;
            recordFlags = Integer.MAX_VALUE;
            rel.getVersions().stream().forEach((rv) -> {
                int typeSequence = Get.identifierService()
                        .getConceptSequence(rel.getTypeNid());
                if (lastRelCharacteristic == Integer.MAX_VALUE) {
                    lastRelCharacteristic = rv.getCharacteristicNid();
                } else if (lastRelCharacteristic != rv.getCharacteristicNid()) {
                    lastRelCharacteristic = rv.getCharacteristicNid();
                    // Characteristic changed, so we need to add a retirement record.
                    int newStamp = cradle.getStamp(Status.INACTIVE,
                            rv.getTime() - 1,
                            rv.getAuthorNid(),
                            rv.getModuleNid(),
                            rv.getPathNid());
                    parentTaxonomyRecord.getTaxonomyRecordUnpacked()
                            .addStampRecord(destinationSequence, typeSequence, 
                                    newStamp, recordFlags);
                }
                if (rv.getTypeNid() == isaNid) {
                    recordFlags = 0;
                    if (rv.getCharacteristicNid() == statedNid) {
                        recordFlags |= TaxonomyFlags.STATED.bits;
                    } else if (rv.getCharacteristicNid() == inferredNid) {
                        recordFlags |= TaxonomyFlags.INFERRED.bits;
                    }
                    parentTaxonomyRecord.getTaxonomyRecordUnpacked()
                            .addStampRecord(destinationSequence, typeSequence, 
                                    rv.getStamp(), recordFlags);
                } else {
                    recordFlags = 0;
                    if (rv.getCharacteristicNid() == statedNid) {
                        recordFlags |= TaxonomyFlags.STATED.bits;
                    } else if (rv.getCharacteristicNid() == inferredNid) {
                        recordFlags |= TaxonomyFlags.INFERRED.bits;
                    } else {
                        recordFlags |= TaxonomyFlags.NON_DL_REL.bits;
                    }
                    parentTaxonomyRecord.getTaxonomyRecordUnpacked()
                            .addStampRecord(destinationSequence, typeSequence, 
                                    rv.getStamp(), recordFlags);
                }
            });
            return destinationSequence;
        }).forEach((destinationSequence) -> {
            destinationOriginRecordSet.add(
                    new DestinationOriginRecord(destinationSequence, 
                            originSequence));
        });
    }

}
