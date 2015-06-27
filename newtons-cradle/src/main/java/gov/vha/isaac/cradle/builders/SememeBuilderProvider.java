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

import gov.vha.isaac.ochre.api.IdentifiedComponentBuilder;
import gov.vha.isaac.ochre.api.logic.LogicalExpression;
import gov.vha.isaac.ochre.api.component.sememe.SememeBuilder;
import gov.vha.isaac.ochre.api.component.sememe.SememeBuilderService;
import gov.vha.isaac.ochre.api.component.sememe.SememeChronology;
import gov.vha.isaac.ochre.api.component.sememe.SememeType;
import gov.vha.isaac.ochre.api.component.sememe.version.DescriptionSememe;
import org.jvnet.hk2.annotations.Service;

/**
 *
 * @author kec
 */
@Service
public class SememeBuilderProvider implements SememeBuilderService {

    @Override
    public SememeBuilder getComponentSememeBuilder(int memeComponentNid, IdentifiedComponentBuilder referencedComponent, int assemblageConceptSequence) {
        return new SememeBuilderImpl(referencedComponent, assemblageConceptSequence, SememeType.COMPONENT_NID, new Object[] {memeComponentNid});
    }

    @Override
    public SememeBuilder getComponentSememeBuilder(int memeComponentNid, int referencedComponentNid, int assemblageConceptSequence) {
        return new SememeBuilderImpl(referencedComponentNid, assemblageConceptSequence, SememeType.COMPONENT_NID, new Object[] {memeComponentNid});
    }

    @Override
    public SememeBuilder getLongSememeBuilder(long longValue, IdentifiedComponentBuilder referencedComponent, int assemblageConceptSequence) {
        return new SememeBuilderImpl(referencedComponent, assemblageConceptSequence, SememeType.LONG, new Object[] {longValue});
    }

    @Override
    public SememeBuilder getLongSememeBuilder(long longValue, int referencedComponentNid, int assemblageConceptSequence) {
        return new SememeBuilderImpl(referencedComponentNid, assemblageConceptSequence, SememeType.LONG, new Object[] {longValue});
    }

    @Override
    public SememeBuilder getLogicalExpressionSememeBuilder(LogicalExpression expression, IdentifiedComponentBuilder referencedComponent, int assemblageConceptSequence) {
        return new SememeBuilderImpl(referencedComponent, assemblageConceptSequence, SememeType.LOGIC_GRAPH, new Object[] {expression});
    }

    @Override
    public SememeBuilder getLogicalExpressionSememeBuilder(LogicalExpression expression, int referencedComponentNid, int assemblageConceptSequence) {
        return new SememeBuilderImpl(referencedComponentNid, assemblageConceptSequence, SememeType.LOGIC_GRAPH, new Object[] {expression});
    }

    @Override
    public SememeBuilder getMembershipSememeBuilder(IdentifiedComponentBuilder referencedComponent, int assemblageConceptSequence) {
        return new SememeBuilderImpl(referencedComponent, assemblageConceptSequence, SememeType.MEMBER, new Object[] {});
    }

    @Override
    public SememeBuilder getMembershipSememeBuilder(int referencedComponentNid, int assemblageConceptSequence) {
        return new SememeBuilderImpl(referencedComponentNid, assemblageConceptSequence, SememeType.MEMBER, new Object[] {});
    }

    @Override
    public SememeBuilder getStringSememeBuilder(String memeString, IdentifiedComponentBuilder referencedComponent, int assemblageConceptSequence) {
        return new SememeBuilderImpl(referencedComponent, assemblageConceptSequence, SememeType.STRING, new Object[] {memeString});
    }

    @Override
    public SememeBuilder getStringSememeBuilder(String memeString, int referencedComponentNid, int assemblageConceptSequence) {
        return new SememeBuilderImpl(referencedComponentNid, assemblageConceptSequence, SememeType.STRING, new Object[] {memeString});
    }

    @Override
    public SememeBuilder<? extends SememeChronology<? extends DescriptionSememe>> getDescriptionSememeBuilder(int caseSignificanceConceptSequence, 
            int descriptionTypeConceptSequence, 
            int languageConceptSequence, 
            String text, 
            IdentifiedComponentBuilder referencedComponent, 
            int assemblageConceptSequence) {
        return new SememeBuilderImpl(referencedComponent, assemblageConceptSequence, 
                SememeType.DESCRIPTION, new Object[] {caseSignificanceConceptSequence, 
                    languageConceptSequence, descriptionTypeConceptSequence, text});
    }

    @Override
    public SememeBuilder<? extends SememeChronology<? extends DescriptionSememe>> getDescriptionSememeBuilder(
            int caseSignificanceConceptSequence, 
            int languageConceptSequence, 
            int descriptionTypeConceptSequence, 
            String text, 
            int referencedComponentNid, int assemblageConceptSequence) {
        return new SememeBuilderImpl(referencedComponentNid, assemblageConceptSequence, 
                SememeType.DESCRIPTION, new Object[] {caseSignificanceConceptSequence, 
                    languageConceptSequence, descriptionTypeConceptSequence, text});
    }
    
    
    
}
