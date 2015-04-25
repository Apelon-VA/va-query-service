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
package gov.vha.isaac.cradle.builders;

import gov.vha.isaac.ochre.api.DataTarget;
import gov.vha.isaac.ochre.api.IdentifiedComponentBuilder;
import gov.vha.isaac.ochre.api.State;
import gov.vha.isaac.ochre.api.commit.ChangeCheckerMode;
import gov.vha.isaac.ochre.api.coordinate.EditCoordinate;
import gov.vha.isaac.ochre.api.logic.LogicalExpression;
import gov.vha.isaac.ochre.api.sememe.SememeBuilder;
import gov.vha.isaac.ochre.api.sememe.SememeChronicle;
import gov.vha.isaac.ochre.api.sememe.SememeType;
import gov.vha.isaac.ochre.model.sememe.SememeChronicleImpl;
import gov.vha.isaac.ochre.model.sememe.version.ComponentNidSememeImpl;
import gov.vha.isaac.ochre.model.sememe.version.ConceptSequenceSememeImpl;
import gov.vha.isaac.ochre.model.sememe.version.ConceptSequenceTimeSememeImpl;
import gov.vha.isaac.ochre.model.sememe.version.LogicGraphSememeImpl;
import gov.vha.isaac.ochre.model.sememe.version.SememeVersionImpl;
import gov.vha.isaac.ochre.model.sememe.version.StringSememeImpl;
import org.ihtsdo.otf.tcc.api.spec.ConceptSpec;

/**
 *
 * @author kec
 */
public class SememeBuilderImpl extends ComponentBuilder<SememeChronicle> implements SememeBuilder {

    IdentifiedComponentBuilder referencedComponentBuilder;
    int referencedComponentNid = Integer.MAX_VALUE;
    
    int assemblageConceptSequence;
    SememeType sememeType;
    Object[] parameters;

    public SememeBuilderImpl(IdentifiedComponentBuilder referencedComponentBuilder, 
            int assemblageConceptSequence, 
            SememeType sememeType, Object... paramaters) {
        this.referencedComponentBuilder = referencedComponentBuilder;
        this.assemblageConceptSequence = assemblageConceptSequence;
        this.sememeType = sememeType;
        this.parameters = paramaters;
    }
    public SememeBuilderImpl(int referencedComponentNid, 
            int assemblageConceptSequence, 
            SememeType sememeType, Object... paramaters) {
        this.referencedComponentNid = referencedComponentNid;
        this.assemblageConceptSequence = assemblageConceptSequence;
        this.sememeType = sememeType;
        this.parameters = paramaters;
    }
    
    

    @Override
    public SememeChronicle build(EditCoordinate editCoordinate, ChangeCheckerMode changeCheckerMode) throws IllegalStateException {
        if (referencedComponentNid == Integer.MAX_VALUE) {
            referencedComponentNid = getIdentifierService().getNidForUuids(referencedComponentBuilder.getUuids());
        }
        SememeChronicleImpl sememeChronicle = new SememeChronicleImpl(sememeType, 
                primordialUuid, 
                getIdentifierService().getNidForUuids(this.getUuids()), 
            assemblageConceptSequence, 
            referencedComponentNid, 
            getIdentifierService().getSememeSequenceForUuids(this.getUuids()));
        sememeChronicle.setAdditionalUuids(additionalUuids);
        getIdentifierService().setConceptSequenceForComponentNid(assemblageConceptSequence, sememeChronicle.getNid());
        switch (sememeType) {
            case COMPONENT_NID:
                ComponentNidSememeImpl cnsi = (ComponentNidSememeImpl) 
                        sememeChronicle.createMutableUncommittedVersion(ComponentNidSememeImpl.class, State.ACTIVE, editCoordinate);
                cnsi.setComponentNid((Integer) parameters[0]);
                break;
            case CONCEPT_SEQUENCE:
                ConceptSequenceSememeImpl cssi = (ConceptSequenceSememeImpl) 
                        sememeChronicle.createMutableUncommittedVersion(ConceptSequenceSememeImpl.class, State.ACTIVE, editCoordinate);
                cssi.setConceptSequence(((ConceptSpec) parameters[0]).getSequence());
                break;
            case CONCEPT_SEQUENCE_TIME:
                ConceptSequenceTimeSememeImpl cstsi = (ConceptSequenceTimeSememeImpl) 
                        sememeChronicle.createMutableUncommittedVersion(ConceptSequenceTimeSememeImpl.class, State.ACTIVE, editCoordinate);
                cstsi.setConceptSequence(((ConceptSpec) parameters[0]).getSequence());
                cstsi.setSememeTime((Long) parameters[1]);
                break;
            case LOGIC_GRAPH:
                LogicGraphSememeImpl lgsi = (LogicGraphSememeImpl) 
                        sememeChronicle.createMutableUncommittedVersion(LogicGraphSememeImpl.class, State.ACTIVE, editCoordinate);
                lgsi.setGraphData(((LogicalExpression) parameters[0]).getData(DataTarget.INTERNAL));
                break;
            case MEMBER:
                SememeVersionImpl svi = (SememeVersionImpl)
                        sememeChronicle.createMutableUncommittedVersion(LogicGraphSememeImpl.class, State.ACTIVE, editCoordinate);
                break;
            case STRING:
                StringSememeImpl ssi = (StringSememeImpl)
                    sememeChronicle.createMutableUncommittedVersion(LogicGraphSememeImpl.class, State.ACTIVE, editCoordinate);
                ssi.setString((String) parameters[0]);
                break;
            case DYNAMIC:
                default:
                    throw new UnsupportedOperationException("Can't handle: " + 
                            sememeType);
        }
        
        if (changeCheckerMode == ChangeCheckerMode.ACTIVE) {
            getCommitService().addUncommitted(sememeChronicle);
        } else {
            getCommitService().addUncommittedNoChecks(sememeChronicle);
        }
        
        return sememeChronicle;
    }
    
}
