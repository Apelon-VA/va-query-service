/*
 * Copyright 2013 International Health Terminology Standards Development Organisation.
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
package org.ihtsdo.otf.query.implementation.clauses;

import gov.vha.isaac.ochre.api.chronicle.LatestVersion;
import gov.vha.isaac.ochre.api.component.sememe.version.DescriptionSememe;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import gov.vha.isaac.ochre.collections.NidSet;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import org.ihtsdo.otf.tcc.api.concept.ConceptVersionBI;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.query.implementation.Clause;
import org.ihtsdo.otf.query.implementation.ClauseComputeType;
import org.ihtsdo.otf.query.implementation.ClauseSemantic;
import org.ihtsdo.otf.query.implementation.ParentClause;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.query.implementation.WhereClause;
import org.ihtsdo.otf.tcc.api.store.Ts;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Retrieves the preferred names for a result set of concepts.
 *
 * @author dylangrald
 */
@XmlRootElement
@XmlAccessorType(value = XmlAccessType.NONE)
public class PreferredNameForConcept extends ParentClause {

    public PreferredNameForConcept(Query enclosingQuery, Clause child) {
        super(enclosingQuery, child);

    }
    protected PreferredNameForConcept() {
    }
    @Override
    public WhereClause getWhereClause() {
        WhereClause whereClause = new WhereClause();
        whereClause.setSemantic(ClauseSemantic.PREFERRED_NAME_FOR_CONCEPT);
        for (Clause clause : getChildren()) {
            whereClause.getChildren().add(clause.getWhereClause());
        }
        return whereClause;
    }

    @Override
    public NidSet computePossibleComponents(NidSet incomingPossibleConcepts) {
        return incomingPossibleConcepts;
    }
    
    @Override
    public EnumSet<ClauseComputeType> getComputePhases(){
        return POST_ITERATION;
    }

    @Override
    public NidSet computeComponents(NidSet incomingConcepts) {
        ViewCoordinate viewCoordinate = getEnclosingQuery().getViewCoordinate();
        NidSet outgoingPreferredNids = new NidSet();
        for (Clause childClause : getChildren()) {
            NidSet childPossibleComponentNids = childClause.computePossibleComponents(incomingConcepts);
            ConceptSequenceSet conceptSequenceSet = ConceptSequenceSet.of(childPossibleComponentNids);
            conceptService.getConceptChronologyStream(conceptSequenceSet)
                    .forEach((conceptChronology) -> {
                        Optional<LatestVersion<DescriptionSememe>> desc = conceptChronology.getPreferredDescription(viewCoordinate, viewCoordinate);
                        if (desc.isPresent()) {
                            outgoingPreferredNids.add(desc.get().value().getNid());
                        }
                    });
        }
        return outgoingPreferredNids;
    }
}
