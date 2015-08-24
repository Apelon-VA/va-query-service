/*
 * Copyright 2015 U.S. Department of Veterans Affairs.
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
package gov.vha.isaac.cradle.tasks;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.cradle.taxonomy.DestinationOriginRecord;
import gov.vha.isaac.cradle.taxonomy.TaxonomyFlags;
import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordPrimitive;
import gov.vha.isaac.cradle.waitfree.CasSequenceObjectMap;
import gov.vha.isaac.metadata.coordinates.LanguageCoordinates;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.Get;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.component.sememe.SememeBuilder;
import gov.vha.isaac.ochre.api.component.sememe.SememeBuilderService;
import gov.vha.isaac.ochre.api.component.sememe.SememeChronology;
import gov.vha.isaac.ochre.api.component.sememe.version.DescriptionSememe;
import gov.vha.isaac.ochre.api.component.sememe.version.DynamicSememe;
import gov.vha.isaac.ochre.api.component.sememe.version.MutableComponentNidSememe;
import gov.vha.isaac.ochre.api.component.sememe.version.MutableLongSememe;
import gov.vha.isaac.ochre.api.component.sememe.version.MutableSememeVersion;
import gov.vha.isaac.ochre.api.component.sememe.version.MutableStringSememe;
import gov.vha.isaac.ochre.api.component.sememe.version.SememeVersion;
import gov.vha.isaac.ochre.model.concept.ConceptChronologyImpl;
import gov.vha.isaac.ochre.model.sememe.SememeChronologyImpl;
import gov.vha.isaac.ochre.model.sememe.version.DescriptionSememeImpl;
import gov.vha.isaac.ochre.model.sememe.version.DynamicSememeImpl;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ihtsdo.otf.tcc.api.id.UuidIdBI;
import org.ihtsdo.otf.tcc.dto.TtkConceptChronicle;
import org.ihtsdo.otf.tcc.dto.TtkConceptLock;
import org.ihtsdo.otf.tcc.dto.component.TtkRevision;
import org.ihtsdo.otf.tcc.dto.component.attribute.TtkConceptAttributesChronicle;
import org.ihtsdo.otf.tcc.dto.component.attribute.TtkConceptAttributesRevision;
import org.ihtsdo.otf.tcc.dto.component.description.TtkDescriptionChronicle;
import org.ihtsdo.otf.tcc.dto.component.description.TtkDescriptionRevision;
import org.ihtsdo.otf.tcc.dto.component.identifier.TtkIdentifier;
import org.ihtsdo.otf.tcc.dto.component.refex.TtkRefexAbstractMemberChronicle;
import org.ihtsdo.otf.tcc.dto.component.refex.type_member.TtkRefexMemberChronicle;
import org.ihtsdo.otf.tcc.dto.component.refex.type_member.TtkRefexRevision;
import org.ihtsdo.otf.tcc.dto.component.refex.type_uuid.TtkRefexUuidMemberChronicle;
import org.ihtsdo.otf.tcc.dto.component.refex.type_uuid.TtkRefexUuidRevision;
import org.ihtsdo.otf.tcc.dto.component.refexDynamic.TtkRefexDynamicMemberChronicle;
import org.ihtsdo.otf.tcc.dto.component.refexDynamic.TtkRefexDynamicRevision;
import org.ihtsdo.otf.tcc.dto.component.refexDynamic.data.TtkRefexDynamicData;

/**
 *
 * @author kec
 */
public class ImportEConceptOchreModel implements Callable<Void> {

    private static final Logger log = LogManager.getLogger();

    private static final CradleExtensions cradle = LookupService.getService(CradleExtensions.class);
    private static final SememeBuilderService sememeBuilderService = LookupService.getService(SememeBuilderService.class);

    private final int descriptionAssemblageSequence;
    public CasSequenceObjectMap<TaxonomyRecordPrimitive> originDestinationTaxonomyRecords;
    public ConcurrentSkipListSet<DestinationOriginRecord> destinationOriginRecordSet;

    {
        originDestinationTaxonomyRecords = cradle.getOriginDestinationTaxonomyMap();
        destinationOriginRecordSet = cradle.getDestinationOriginRecordSet();
        descriptionAssemblageSequence = Get.identifierService().getConceptSequenceForUuids(
                IsaacMetadataAuxiliaryBinding.DESCRIPTION_ASSEMBLAGE.getUuids());
    }

    private TtkConceptChronicle eConcept;
    private UUID newPathUuid = null;

    public ImportEConceptOchreModel(TtkConceptChronicle eConcept, UUID newPathUuid) {
        this(eConcept);
        this.newPathUuid = newPathUuid;
    }

    public ImportEConceptOchreModel(TtkConceptChronicle eConcept) {
        this.eConcept = eConcept;
    }

    @Override
    public Void call() throws Exception {

        TtkConceptLock.getLock(eConcept.getUuidList()).lock();
        try {
            if (this.newPathUuid != null) {
                eConcept.processComponentRevisions(r -> r.setPathUuid(newPathUuid));
            }

            ConceptChronologyImpl conceptChronology
                    = (ConceptChronologyImpl) Get.conceptService().getConcept(eConcept.getUuidList().toArray(new UUID[0]));
            int conceptSequence = conceptChronology.getConceptSequence();

            TtkConceptAttributesChronicle attributes = eConcept.getConceptAttributes();
            if (attributes != null) {
                List<?> builtObjects = new ArrayList<>();
                attributes.getAdditionalIdComponents().forEach((additionalId) -> {
                    int assemblageConceptSequence = Get.identifierService().getConceptSequenceForUuids(additionalId.getAuthorityUuid());
                    int idStampSequence = additionalId.getStampSequence();
                    switch (additionalId.getIdType()) {
                        case LONG:
                        case STRING:
                            SememeBuilder<? extends SememeChronology<? extends SememeVersion<?>>> builder = 
                                    Get.sememeBuilderService().getStringSememeBuilder(additionalId.getDenotation().toString(), conceptChronology.getNid(), assemblageConceptSequence);
                            builder.build(idStampSequence, builtObjects);
                            break;
                    }

                });
                builtObjects.forEach((idSememe) -> {
                    Get.sememeService().writeSememe((SememeChronology<?>) idSememe);
                });
                TaxonomyRecordPrimitive parentTaxonomyRecord;
                if (originDestinationTaxonomyRecords.containsKey(conceptSequence)) {
                    parentTaxonomyRecord = originDestinationTaxonomyRecords.get(conceptSequence).get();
                } else {
                    parentTaxonomyRecord = new TaxonomyRecordPrimitive();
                }
                int versionStampSequence = Get.commitService().getStampSequence(attributes.status.getState(),
                        attributes.time,
                        Get.identifierService().getConceptSequenceForUuids(attributes.authorUuid),
                        Get.identifierService().getConceptSequenceForUuids(attributes.moduleUuid),
                        Get.identifierService().getConceptSequenceForUuids(attributes.pathUuid));
                conceptChronology.createMutableVersion(versionStampSequence);
                parentTaxonomyRecord.getTaxonomyRecordUnpacked().addStampRecord(conceptSequence,
                        conceptSequence, versionStampSequence, TaxonomyFlags.CONCEPT_STATUS.bits);
                if (attributes.revisions != null) {
                    for (TtkConceptAttributesRevision revision : attributes.revisions) {
                        versionStampSequence = getStampSequence(revision);
                        conceptChronology.createMutableVersion(versionStampSequence);
                        parentTaxonomyRecord.getTaxonomyRecordUnpacked().addStampRecord(conceptSequence,
                                conceptSequence, versionStampSequence, TaxonomyFlags.CONCEPT_STATUS.bits);
                    }
                }
                originDestinationTaxonomyRecords.put(conceptSequence, parentTaxonomyRecord);
                
                for (TtkIdentifier id : attributes.getAdditionalIdComponents()) {
                    
                    switch (id.getIdType()) {
                        case LONG:
                            SememeBuilder<SememeChronology<MutableLongSememe<?>>> longIdBuilder =  
                                sememeBuilderService.getLongSememeBuilder((Long)id.getDenotation(), 
                                        conceptChronology.getNid(), 
                                        Get.identifierService().getConceptSequenceForUuids(id.getAuthorityUuid()));
                            
                            SememeChronology<MutableLongSememe<?>> idSememe = longIdBuilder.build(getStampSequence(id), new ArrayList());
                            Get.sememeService().writeSememe(idSememe);
                            break;
                        case STRING:
                            SememeBuilder<SememeChronology<MutableStringSememe<?>>> stringIdBuilder =  
                                sememeBuilderService.getStringSememeBuilder((String)id.getDenotation(), 
                                    conceptChronology.getNid(), 
                                    Get.identifierService().getConceptSequenceForUuids(id.getAuthorityUuid()));
                        
                            SememeChronology<MutableStringSememe<?>> stringSememe = stringIdBuilder.build(getStampSequence(id), new ArrayList());
                            Get.sememeService().writeSememe(stringSememe);
                            break;
                        case UUID:
                            //TODO Keith - is this the right thing to do?
                            if (id.getAuthorityUuid().equals(IsaacMetadataAuxiliaryBinding.GENERATED_UUID.getPrimodialUuid()))
                            {
                                conceptChronology.addAdditionalUuids((UUID)id.getDenotation());
                                break;
                            }
                            
                        default :
                            throw new UnsupportedOperationException("Unhandled case - id attribute " + id.toString() + " on concept " + conceptChronology.toExternalString());
                    }
                }
            }

            Get.conceptService().writeConcept(conceptChronology);

            for (TtkDescriptionChronicle desc : eConcept.getDescriptions()) {
                int caseSignificanceConceptSequence = LanguageCoordinates.caseSignificanceToConceptSequence(desc.initialCaseSignificant);
                int languageConceptSequence = LanguageCoordinates.iso639toConceptSequence(desc.getLang());
                assert languageConceptSequence == IsaacMetadataAuxiliaryBinding.ENGLISH.getConceptSequence() : "Converting:  " + desc.getLang() + " " + desc;
                int descriptionTypeConceptSequence = Get.identifierService().getConceptSequenceForUuids(desc.getTypeUuid());
                if (descriptionTypeConceptSequence == IsaacMetadataAuxiliaryBinding.PREFERRED.getConceptSequence()
                        || descriptionTypeConceptSequence == IsaacMetadataAuxiliaryBinding.ACCEPTABLE.getConceptSequence()) {
                    log.error("Found incorrect descripiton type: " + desc);
                }
                assert descriptionTypeConceptSequence == IsaacMetadataAuxiliaryBinding.FULLY_SPECIFIED_NAME.getConceptSequence()
                        || descriptionTypeConceptSequence == IsaacMetadataAuxiliaryBinding.PREFERRED.getConceptSequence()
                        || descriptionTypeConceptSequence == IsaacMetadataAuxiliaryBinding.ACCEPTABLE.getConceptSequence()
                        || descriptionTypeConceptSequence == IsaacMetadataAuxiliaryBinding.SYNONYM.getConceptSequence()
                        || descriptionTypeConceptSequence == IsaacMetadataAuxiliaryBinding.DEFINITION_DESCRIPTION_TYPE.getConceptSequence() : "Converting: " + desc.getTypeUuid()
                        + " " + desc;

                SememeBuilder<? extends SememeChronology<? extends DescriptionSememe>> descBuilder
                        = sememeBuilderService.getDescriptionSememeBuilder(caseSignificanceConceptSequence,
                                languageConceptSequence,
                                descriptionTypeConceptSequence,
                                desc.text,
                                conceptChronology.getNid(),
                                descriptionAssemblageSequence);
                descBuilder.setPrimordialUuid(desc.getPrimordialUuid());

                desc.getAdditionalIdComponents().forEach((additionalId) -> {
                    switch (additionalId.getIdType()) {
                        case UUID:
                            descBuilder.addUuids(((UuidIdBI) additionalId).getDenotation());
                            break;
                         
                    }

                });
                desc.getAdditionalIdComponents().forEach((additionalId) -> {
                    switch (additionalId.getIdType()) {
                        case STRING:
                        case LONG:
                            int assemblageConceptSequence = Get.identifierService().getConceptSequenceForUuids(additionalId.getAuthorityUuid());
                            int idStampSequence = additionalId.getStampSequence();
                            SememeBuilder<? extends SememeChronology<? extends SememeVersion<?>>> builder = 
                                    Get.sememeBuilderService().getStringSememeBuilder(additionalId.getDenotation().toString(), 
                                            descBuilder, 
                                            assemblageConceptSequence);
                            
                      List<?> builtObjects = new ArrayList<>();
                            
                            builder.build(idStampSequence, builtObjects);
                            builtObjects.forEach((idSememe) -> {
                                Get.sememeService().writeSememe((SememeChronology<?>) idSememe);
                            });
                           
                    }

                });

                int stampSequence = getStampSequence(desc);
                List createdComponents = new ArrayList();
                SememeChronologyImpl newDescription = (SememeChronologyImpl) descBuilder.build(stampSequence, createdComponents);
                if (desc.revisions != null) {
                    for (TtkDescriptionRevision revision : desc.revisions) {
                        stampSequence = getStampSequence(revision);
                        DescriptionSememeImpl version = (DescriptionSememeImpl) newDescription.createMutableVersion(DescriptionSememeImpl.class, stampSequence);
                        version.setCaseSignificanceConceptSequence(LanguageCoordinates.caseSignificanceToConceptSequence(revision.initialCaseSignificant));
                        version.setDescriptionTypeConceptSequence(Get.identifierService().getConceptSequenceForUuids(revision.getTypeUuid()));
                        version.setLanguageConceptSequence(LanguageCoordinates.iso639toConceptSequence(revision.lang));
                        version.setText(revision.text);
                    }
                }
                Get.sememeService().writeSememe(newDescription);
                for (TtkRefexAbstractMemberChronicle<?> annotations : desc.getAnnotations()) {
                    makeSememe(annotations);
                }
                for (TtkRefexDynamicMemberChronicle annotations : desc.getAnnotationsDynamic()) {
                    makeSememe(annotations);
                }
            }

            eConcept.getRefsetMembers().stream().forEach((TtkRefexAbstractMemberChronicle member) -> {
                makeSememe(member);
            });
            eConcept.getRefsetMembersDynamic().stream().forEach((TtkRefexDynamicMemberChronicle member) -> {
                makeSememe(member);
            });
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Failure importing " + eConcept.toString());
            throw e;
        } finally {
            TtkConceptLock.getLock(eConcept.getUuidList()).unlock();
        }
    }

    private static <E extends TtkRevision> Stream<E> stream(List<E> collection) {
        if (collection == null) {
            return Stream.empty();
        }
        return collection.stream();
    }
    
    public static void makeSememe(TtkRefexDynamicMemberChronicle member) throws UnsupportedOperationException, IllegalStateException {
        int referencedComponentNid = Get.identifierService().getNidForUuids(member.getComponentUuid());
        int assemblageSequence = Get.identifierService().getConceptSequenceForUuids(member.getRefexAssemblageUuid());
        int stampSequence = getStampSequence(member);

        SememeBuilder<SememeChronology<DynamicSememe<?>>> sememeBuilder = sememeBuilderService
                .getDyanmicSememeBuilder(referencedComponentNid, assemblageSequence, TtkRefexDynamicData.convertFromTTK(member.getData()));

        SememeChronology<DynamicSememe<?>> sememe = sememeBuilder.build(stampSequence, new ArrayList<>());
        stream(member.revisions).forEach((TtkRefexDynamicRevision r) -> 
        {
            int revisionStampSequence = getStampSequence(r);
            DynamicSememeImpl mutable = (DynamicSememeImpl)sememe.createMutableVersion(DynamicSememe.class, revisionStampSequence);
            mutable.setData(TtkRefexDynamicData.convertFromTTK(r.getData()));
        });
        Get.sememeService().writeSememe(sememe);
        
        for (TtkRefexDynamicMemberChronicle nested : member.getAnnotationsDynamic()) {
            makeSememe(nested);
        }
    }

    private void makeSememe(TtkRefexAbstractMemberChronicle member) throws UnsupportedOperationException, IllegalStateException {
        int referencedComponentNid = Get.identifierService().getNidForUuids(member.referencedComponentUuid);
        int assemblageSequence = Get.identifierService().getConceptSequenceForUuids(member.assemblageUuid);
        int stampSequence = getStampSequence(member);
        switch (member.getType()) {
            case CID:
                TtkRefexUuidMemberChronicle cidMember = (TtkRefexUuidMemberChronicle) member;
                SememeBuilder<SememeChronology<MutableComponentNidSememe<?>>> cidBuilder
                        = sememeBuilderService.getComponentSememeBuilder(Get.identifierService().getNidForUuids(cidMember.uuid1),
                                referencedComponentNid,
                                assemblageSequence);
                SememeChronology<MutableComponentNidSememe<?>> cidSememe = cidBuilder.build(stampSequence, new ArrayList());
                stream(cidMember.revisions).forEach((TtkRefexUuidRevision r) -> {
                    int revisionStampSequence = getStampSequence(r);
                    MutableComponentNidSememe mutable = cidSememe.createMutableVersion(MutableComponentNidSememe.class, revisionStampSequence);
                    mutable.setComponentNid(Get.identifierService().getNidForUuids(r.uuid1));
                });
                Get.sememeService().writeSememe(cidSememe);
                break;

            case MEMBER:
                TtkRefexMemberChronicle memberMember = (TtkRefexMemberChronicle) member;
                SememeBuilder<SememeChronology<MutableSememeVersion<?>>> mb
                        = sememeBuilderService.getMembershipSememeBuilder(referencedComponentNid,
                                assemblageSequence);
                SememeChronology<MutableSememeVersion<?>> memberSememe = mb.build(stampSequence, new ArrayList());

                stream(memberMember.revisions).forEach((TtkRefexRevision r) -> {
                    int revisionStampSequence = getStampSequence(r);
                    memberSememe.createMutableVersion(MutableSememeVersion.class, revisionStampSequence);
                });
                Get.sememeService().writeSememe(memberSememe);
                break;

            case ARRAY_BYTEARRAY:
            case BOOLEAN:
            case CID_BOOLEAN:
            case CID_CID:
            case CID_CID_CID:
            case CID_CID_CID_FLOAT:
            case CID_CID_CID_INT:
            case CID_CID_CID_LONG:
            case CID_CID_CID_STRING:
            case CID_CID_STR:
            case CID_FLOAT:
            case CID_INT:
            case CID_LONG:
            case CID_STR:
            case INT:
            case LOGIC:
            case LONG:
            case STR:
            case UNKNOWN:
            default:
                throw new UnsupportedOperationException("can't handle: " + member.getType());
        }
    }

    private static int getStampSequence(TtkRevision revision) {
        int versionStampSequence;
        versionStampSequence = Get.commitService().getStampSequence(revision.status.getState(),
                revision.time,
                Get.identifierService().getConceptSequenceForUuids(revision.authorUuid),
                Get.identifierService().getConceptSequenceForUuids(revision.moduleUuid),
                Get.identifierService().getConceptSequenceForUuids(revision.pathUuid));
        return versionStampSequence;
    }

}
