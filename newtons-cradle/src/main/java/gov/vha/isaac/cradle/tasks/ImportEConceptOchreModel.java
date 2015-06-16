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
import gov.vha.isaac.ochre.api.IdentifierService;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.commit.CommitService;
import gov.vha.isaac.ochre.api.component.concept.ConceptService;
import gov.vha.isaac.ochre.api.component.concept.ConceptServiceManagerI;
import gov.vha.isaac.ochre.api.component.sememe.SememeBuilder;
import gov.vha.isaac.ochre.api.component.sememe.SememeBuilderService;
import gov.vha.isaac.ochre.api.component.sememe.SememeChronology;
import gov.vha.isaac.ochre.api.component.sememe.SememeService;
import gov.vha.isaac.ochre.api.component.sememe.version.DescriptionSememe;
import gov.vha.isaac.ochre.api.component.sememe.version.MutableComponentNidSememe;
import gov.vha.isaac.ochre.api.component.sememe.version.MutableSememeVersion;
import gov.vha.isaac.ochre.model.concept.ConceptChronologyImpl;
import gov.vha.isaac.ochre.model.sememe.SememeChronologyImpl;
import gov.vha.isaac.ochre.model.sememe.version.DescriptionSememeImpl;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ihtsdo.otf.tcc.dto.TtkConceptChronicle;
import org.ihtsdo.otf.tcc.dto.component.TtkRevision;
import org.ihtsdo.otf.tcc.dto.component.attribute.TtkConceptAttributesChronicle;
import org.ihtsdo.otf.tcc.dto.component.attribute.TtkConceptAttributesRevision;
import org.ihtsdo.otf.tcc.dto.component.description.TtkDescriptionChronicle;
import org.ihtsdo.otf.tcc.dto.component.description.TtkDescriptionRevision;
import org.ihtsdo.otf.tcc.dto.component.refex.TtkRefexAbstractMemberChronicle;
import org.ihtsdo.otf.tcc.dto.component.refex.type_member.TtkRefexMemberChronicle;
import org.ihtsdo.otf.tcc.dto.component.refex.type_member.TtkRefexRevision;
import org.ihtsdo.otf.tcc.dto.component.refex.type_uuid.TtkRefexUuidMemberChronicle;
import org.ihtsdo.otf.tcc.dto.component.refex.type_uuid.TtkRefexUuidRevision;

/**
 *
 * @author kec
 */
public class ImportEConceptOchreModel implements Callable<Void> {

    private static final Logger log = LogManager.getLogger();

    private static final CommitService commitService = LookupService.getService(CommitService.class);
    private static final SememeService sememeService = LookupService.getService(SememeService.class);
    private static final IdentifierService identifierService = LookupService.getService(IdentifierService.class);
    private static final ConceptService conceptService = LookupService.getService(ConceptServiceManagerI.class).get();
    private static final CradleExtensions cradle = LookupService.getService(CradleExtensions.class);
    private static final SememeBuilderService sememeBuilderService = LookupService.getService(SememeBuilderService.class);

    private static final int descriptionAssemblageSequence;
    public static CasSequenceObjectMap<TaxonomyRecordPrimitive> originDestinationTaxonomyRecords;
    public static ConcurrentSkipListSet<DestinationOriginRecord> destinationOriginRecordSet;

    static {
        originDestinationTaxonomyRecords = cradle.getOriginDestinationTaxonomyMap();
        destinationOriginRecordSet = cradle.getDestinationOriginRecordSet();
        descriptionAssemblageSequence = identifierService.getConceptSequenceForUuids(
                IsaacMetadataAuxiliaryBinding.DESCRIPTION_ASSEMBLAGE.getUuids());
    }

    TtkConceptChronicle eConcept;
    UUID newPathUuid = null;
    int lastRelCharacteristic = Integer.MAX_VALUE;
    int recordFlags = Integer.MAX_VALUE;

    public ImportEConceptOchreModel(TtkConceptChronicle eConcept, UUID newPathUuid) {
        this(eConcept);
        this.newPathUuid = newPathUuid;
    }

    public ImportEConceptOchreModel(TtkConceptChronicle eConcept) {
        this.eConcept = eConcept;
    }

    @Override
    public Void call() throws Exception {

        try {
            if (this.newPathUuid != null) {
                eConcept.processComponentRevisions(r -> r.setPathUuid(newPathUuid));
            }

            ConceptChronologyImpl conceptChronology
                    = (ConceptChronologyImpl) conceptService.getConcept(eConcept.getUuidList().toArray(new UUID[0]));
            int conceptSequence = conceptChronology.getConceptSequence();

            TtkConceptAttributesChronicle attributes = eConcept.getConceptAttributes();
            if (attributes != null) {
            TaxonomyRecordPrimitive parentTaxonomyRecord;
                if (originDestinationTaxonomyRecords.containsKey(conceptSequence)) {
                    parentTaxonomyRecord = originDestinationTaxonomyRecords.get(conceptSequence).get();
                } else {
                    parentTaxonomyRecord = new TaxonomyRecordPrimitive();
                }
                int versionStampSequence = commitService.getStampSequence(attributes.status.getState(),
                        attributes.time,
                        identifierService.getConceptSequenceForUuids(attributes.authorUuid),
                        identifierService.getConceptSequenceForUuids(attributes.moduleUuid),
                        identifierService.getConceptSequenceForUuids(attributes.pathUuid));
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

            }

            conceptService.writeConcept(conceptChronology);

            for (TtkDescriptionChronicle desc : eConcept.getDescriptions()) {
                int caseSignificanceConceptSequence = LanguageCoordinates.caseSignificanceToConceptSequence(desc.initialCaseSignificant);
                int languageConceptSequence = LanguageCoordinates.iso639toConceptSequence(desc.getLang());
                int descriptionTypeConceptSequence = identifierService.getConceptSequenceForUuids(desc.getTypeUuid());
                SememeBuilder<? extends SememeChronology<? extends DescriptionSememe>> descBuilder
                        = sememeBuilderService.getDescriptionSememeBuilder(caseSignificanceConceptSequence,
                                languageConceptSequence,
                                descriptionTypeConceptSequence,
                                desc.text,
                                conceptChronology.getNid(),
                                descriptionAssemblageSequence);
                int stampSequence = getStampSequence(desc);
                List createdComponents = new ArrayList();
                SememeChronologyImpl<DescriptionSememeImpl> newDescription = (SememeChronologyImpl<DescriptionSememeImpl>) descBuilder.build(stampSequence, createdComponents);
                if (desc.revisions != null) {
                    for (TtkDescriptionRevision revision : desc.revisions) {
                        stampSequence = getStampSequence(revision);
                        DescriptionSememeImpl version = (DescriptionSememeImpl) newDescription.createMutableVersion(DescriptionSememeImpl.class, stampSequence);
                        version.setCaseSignificanceConceptSequence(LanguageCoordinates.caseSignificanceToConceptSequence(revision.initialCaseSignificant));
                        version.setDescriptionTypeConceptSequence(identifierService.getConceptSequenceForUuids(revision.getTypeUuid()));
                        version.setLanguageConceptSequence(LanguageCoordinates.iso639toConceptSequence(revision.lang));
                        version.setText(revision.text);
                    }
                }
                sememeService.writeSememe(newDescription);
            }
            
            eConcept.getRefsetMembers().stream().forEach((TtkRefexAbstractMemberChronicle member) -> {
                makeSememe(member);
            });
            return null;
        } catch (Exception e) {
            System.err.println("Failure importing " + eConcept.toString());
            throw e;
        }
    }
    
    
    private static <E extends TtkRevision> Stream<E> stream(List<E> collection) {
        if (collection == null) {
            return Stream.empty();
        }
        return collection.stream();
    }

    private void makeSememe(TtkRefexAbstractMemberChronicle member) throws UnsupportedOperationException, IllegalStateException {
        int referencedComponentNid = identifierService.getNidForUuids(member.referencedComponentUuid);
        int assemblageSequence = identifierService.getConceptSequenceForUuids(member.assemblageUuid);
        int stampSequence = getStampSequence(member);
        switch (member.getType()) {
            case CID:
                TtkRefexUuidMemberChronicle cidMember = (TtkRefexUuidMemberChronicle) member;
                SememeBuilder<SememeChronology<MutableComponentNidSememe>> cidBuilder =
                        sememeBuilderService.getComponentSememeBuilder(identifierService.getNidForUuids(cidMember.uuid1),
                                referencedComponentNid,
                                assemblageSequence);
                SememeChronology<MutableComponentNidSememe> cidSememe = cidBuilder.build(stampSequence, new ArrayList());
                stream(cidMember.revisions).forEach((TtkRefexUuidRevision r) -> {
                        int revisionStampSequence = getStampSequence(r);
                        MutableComponentNidSememe mutable = cidSememe.createMutableVersion(MutableComponentNidSememe.class, revisionStampSequence);
                        mutable.setComponentNid(identifierService.getNidForUuids(r.uuid1));
                    });
                sememeService.writeSememe(cidSememe);
                break;
                
            case MEMBER:
                TtkRefexMemberChronicle memberMember = (TtkRefexMemberChronicle) member;
                SememeBuilder<SememeChronology<MutableSememeVersion>> mb =
                        sememeBuilderService.getMembershipSememeBuilder(referencedComponentNid,
                                assemblageSequence);
                SememeChronology<MutableSememeVersion> memberSememe = mb.build(stampSequence, new ArrayList());
                
                stream(memberMember.revisions).forEach((TtkRefexRevision r) -> {
                        int revisionStampSequence = getStampSequence(r);
                        memberSememe.createMutableVersion(MutableSememeVersion.class, revisionStampSequence);
                    });
                sememeService.writeSememe(memberSememe);
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
            default: throw new UnsupportedOperationException("can't handle: " + member.getType());
        }
    }

    private int getStampSequence(TtkRevision revision) {
        int versionStampSequence;
        versionStampSequence = commitService.getStampSequence(revision.status.getState(),
                revision.time,
                identifierService.getConceptSequenceForUuids(revision.authorUuid),
                identifierService.getConceptSequenceForUuids(revision.moduleUuid),
                identifierService.getConceptSequenceForUuids(revision.pathUuid));
        return versionStampSequence;
    }

}
