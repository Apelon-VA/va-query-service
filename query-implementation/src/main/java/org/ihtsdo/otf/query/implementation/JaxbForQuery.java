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

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.ihtsdo.otf.query.implementation.clauses.*;
import org.ihtsdo.otf.tcc.api.contradiction.strategy.IdentifyAllConflict;
import org.ihtsdo.otf.tcc.api.contradiction.strategy.LastCommitWins;
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

import java.util.HashMap;
import org.ihtsdo.otf.tcc.api.coordinate.LanguagePreferenceList;

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
                    ViewCoordinate.class,
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