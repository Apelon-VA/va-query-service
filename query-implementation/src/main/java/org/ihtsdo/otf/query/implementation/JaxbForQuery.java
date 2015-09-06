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
package org.ihtsdo.otf.query.implementation;

import gov.vha.isaac.ochre.collections.IntSet;
import gov.vha.isaac.ochre.model.coordinate.LanguageCoordinateImpl;
import gov.vha.isaac.ochre.model.coordinate.LogicCoordinateImpl;
import gov.vha.isaac.ochre.model.coordinate.LogicCoordinateLazyBinding;
import gov.vha.isaac.ochre.model.coordinate.StampCoordinateImpl;
import gov.vha.isaac.ochre.model.coordinate.StampPositionImpl;
import gov.vha.isaac.ochre.model.coordinate.TaxonomyCoordinateImpl;
import gov.vha.isaac.ochre.observable.model.coordinate.ObservableLanguageCoordinateImpl;
import gov.vha.isaac.ochre.observable.model.coordinate.ObservableLogicCoordinateImpl;
import java.util.HashMap;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import org.ihtsdo.otf.query.implementation.clauses.ChangedFromPreviousVersion;
import org.ihtsdo.otf.query.implementation.clauses.ComponentsFromSnomedIds;
import org.ihtsdo.otf.query.implementation.clauses.ConceptForComponent;
import org.ihtsdo.otf.query.implementation.clauses.ConceptIs;
import org.ihtsdo.otf.query.implementation.clauses.ConceptIsChildOf;
import org.ihtsdo.otf.query.implementation.clauses.ConceptIsDescendentOf;
import org.ihtsdo.otf.query.implementation.clauses.ConceptIsKindOf;
import org.ihtsdo.otf.query.implementation.clauses.DescriptionActiveLuceneMatch;
import org.ihtsdo.otf.query.implementation.clauses.DescriptionActiveRegexMatch;
import org.ihtsdo.otf.query.implementation.clauses.DescriptionLuceneMatch;
import org.ihtsdo.otf.query.implementation.clauses.DescriptionRegexMatch;
import org.ihtsdo.otf.query.implementation.clauses.FullySpecifiedNameForConcept;
import org.ihtsdo.otf.query.implementation.clauses.PreferredNameForConcept;
import org.ihtsdo.otf.query.implementation.clauses.RefsetContainsConcept;
import org.ihtsdo.otf.query.implementation.clauses.RefsetContainsKindOfConcept;
import org.ihtsdo.otf.query.implementation.clauses.RefsetContainsString;
import org.ihtsdo.otf.query.implementation.clauses.RefsetLuceneMatch;
import org.ihtsdo.otf.query.implementation.clauses.RelRestriction;
import org.ihtsdo.otf.tcc.api.contradiction.strategy.IdentifyAllConflict;
import org.ihtsdo.otf.tcc.api.contradiction.strategy.LastCommitWins;
import org.ihtsdo.otf.tcc.api.coordinate.LanguagePreferenceList;
import org.ihtsdo.otf.tcc.api.coordinate.Path;
import org.ihtsdo.otf.tcc.api.coordinate.Position;
import org.ihtsdo.otf.tcc.api.coordinate.SimplePath;
import org.ihtsdo.otf.tcc.api.coordinate.SimplePosition;
import org.ihtsdo.otf.tcc.api.coordinate.SimpleViewCoordinate;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.nid.ConcurrentBitSet;
import org.ihtsdo.otf.tcc.api.spec.ConceptSpec;
import org.ihtsdo.otf.tcc.api.spec.RelSpec;
import org.ihtsdo.otf.tcc.ddo.ResultList;
import org.ihtsdo.otf.tcc.ddo.concept.ConceptChronicleDdo;
import org.ihtsdo.otf.tcc.ddo.concept.component.description.DescriptionChronicleDdo;
import org.ihtsdo.otf.tcc.ddo.concept.component.description.DescriptionVersionDdo;
import org.ihtsdo.otf.tcc.ddo.concept.component.description.SimpleDescriptionVersionDdo;
import org.ihtsdo.otf.tcc.model.cc.LanguageSortPrefs;

/**
 *
 * @author kec
 */
public class JaxbForQuery {

    public static JAXBContext singleton;

    public static JAXBContext get() throws JAXBException {
        if (singleton == null) {
            singleton = JAXBContext.newInstance(
                    And.class,
                    AndNot.class,
                    IntSet.class,
                    ViewCoordinate.class,
                    LanguageCoordinateImpl.class,
                    StampCoordinateImpl.class,
                    StampPositionImpl.class,
                    TaxonomyCoordinateImpl.class,
                    LogicCoordinateImpl.class,
                    LogicCoordinateLazyBinding.class,
                    ObservableLogicCoordinateImpl.class,
                    ObservableLanguageCoordinateImpl.class,
                    IdentifyAllConflict.class, 
                    LastCommitWins.class,
                    Where.class,
                    ForSetSpecification.class,
                    ComponentCollectionTypes.class,
                    ConcurrentBitSet.class,
                    Position.class,
                    LetMap.class,
                    Path.class,
                    Query.class,
                    QueryFactory.class,
                    QueryFactory.QueryFromFactory.class,
                    ConceptSpec.class,
                    RelSpec.class,
                    ResultList.class,
                    DescriptionChronicleDdo.class,
                    DescriptionVersionDdo.class,
                    ConceptChronicleDdo.class,
                    SimpleDescriptionVersionDdo.class,
                    SimpleViewCoordinate.class,
                    SimplePath.class,
                    SimplePosition.class, 
                    LanguagePreferenceList.class,
                    LanguageSortPrefs.class, 
                    ReturnTypes.class,
                    HashMap.class,
                    Not.class,
                    Or.class,
                    Xor.class,
                    ParentClause.class,
                    LeafClause.class,
                    Clause.class,
                    ConceptIsKindOf.class,
                    ChangedFromPreviousVersion.class,
                    ComponentsFromSnomedIds.class,
                    ConceptForComponent.class,
                    ConceptIs.class,
                    ConceptIsChildOf.class,
                    ConceptIsDescendentOf.class,
                    ConceptIsKindOf.class,
                    DescriptionActiveLuceneMatch.class,
                    DescriptionActiveRegexMatch.class,
                    DescriptionLuceneMatch.class,
                    DescriptionRegexMatch.class,
                    FullySpecifiedNameForConcept.class,
                    PreferredNameForConcept.class,
                    RefsetContainsConcept.class,
                    RefsetContainsKindOfConcept.class,
                    RefsetContainsString.class,
                    RefsetLuceneMatch.class,
                    RelRestriction.class);
        }
        return singleton;
    }
}
