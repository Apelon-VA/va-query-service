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
package org.ihtsdo.otf.query.implementation.clauses;

import gov.vha.isaac.ochre.api.Get;
import gov.vha.isaac.ochre.api.component.concept.ConceptVersion;
import gov.vha.isaac.ochre.api.coordinate.TaxonomyCoordinate;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import gov.vha.isaac.ochre.collections.NidSet;
import java.util.EnumSet;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import org.ihtsdo.otf.query.implementation.ClauseComputeType;
import org.ihtsdo.otf.query.implementation.ClauseSemantic;
import org.ihtsdo.otf.query.implementation.LeafClause;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.query.implementation.WhereClause;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.spec.ConceptSpec;

/**
 * Allows the user specify a search for circular relationships. Also allows the user to limit the identification
 * of circular relationships to specify types, and also to allow subsumption on the relationship type.
 * 
 * @author kec
 */
@XmlRootElement
@XmlAccessorType(value = XmlAccessType.NONE)
public class RelationshipIsCircular extends LeafClause {

    @XmlElement
    String relTypeKey;
    @XmlElement
    String viewCoordinateKey;
    @XmlElement
    String relTypeSubsumptionKey;

    ConceptSequenceSet relTypeSet;

    public RelationshipIsCircular(Query enclosingQuery, String relTypeKey, 
            String viewCoordinateKey, String relTypeSubsumptionKey) {
        super(enclosingQuery);
        this.relTypeKey = relTypeKey;
        this.viewCoordinateKey = viewCoordinateKey;
        this.relTypeSubsumptionKey = relTypeSubsumptionKey;

    }

    protected RelationshipIsCircular() {
    }

    @Override
    public WhereClause getWhereClause() {
        WhereClause whereClause = new WhereClause();
        whereClause.setSemantic(ClauseSemantic.RELATIONSHIP_IS_CIRCULAR);
        whereClause.getLetKeys().add(relTypeKey);
        whereClause.getLetKeys().add(viewCoordinateKey);
        whereClause.getLetKeys().add(relTypeSubsumptionKey);
        System.out.println("Where clause size: " + whereClause.getLetKeys().size());
        return whereClause;

    }

    @Override
    public EnumSet<ClauseComputeType> getComputePhases() {
        return PRE_ITERATION_AND_ITERATION;
    }

    @Override
    public NidSet computePossibleComponents(NidSet incomingPossibleComponents) {
        System.out.println("Let declerations: " + enclosingQuery.getLetDeclarations());
        TaxonomyCoordinate taxonomyCoordinate = (TaxonomyCoordinate) enclosingQuery.getLetDeclarations().get(viewCoordinateKey);
        ConceptSpec relType = (ConceptSpec) enclosingQuery.getLetDeclarations().get(relTypeKey);
        Boolean relTypeSubsumption = (Boolean) enclosingQuery.getLetDeclarations().get(relTypeSubsumptionKey);

        //The default is to set relTypeSubsumption and destinationSubsumption to true.
        if (relTypeSubsumption == null) {
            relTypeSubsumption = true;
        }

        relTypeSet = new ConceptSequenceSet();
        relTypeSet.add(relType.getConceptSequence());
        if (relTypeSubsumption) {
            relTypeSet.or(Get.taxonomyService().getKindOfSequenceSet(relType.getConceptSequence(), taxonomyCoordinate));
        }

        return incomingPossibleComponents;
    }

    @Override
    public void getQueryMatches(ConceptVersion conceptVersion) {
        throw new UnsupportedOperationException();
        /*TaxonomyCoordinate taxonomyCoordinate = (TaxonomyCoordinate) enclosingQuery.getLetDeclarations().get(viewCoordinateKey);
        Get.taxonomyService().getAllRelationshipDestinationSequencesOfType(
                conceptVersion.getChronology().getConceptSequence(), relTypeSet, viewCoordinate)
                .forEach((destinationSequence) -> {
                    if (destinationSet.contains(destinationSequence)) {
                        getResultsCache().add(conceptVersion.getChronology().getNid());
                    }
                });
                */
    }
}
