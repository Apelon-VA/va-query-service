package org.ihtsdo.otf.query.integration.tests.lucene;

//~--- non-JDK imports --------------------------------------------------------

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
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.ihtsdo.otf.tcc.api.blueprint.ComponentProperty;
import org.ihtsdo.otf.tcc.api.blueprint.DescriptionCAB;
import org.ihtsdo.otf.tcc.api.blueprint.IdDirective;
import org.ihtsdo.otf.tcc.api.blueprint.TerminologyBuilderBI;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.api.coordinate.EditCoordinate;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.description.DescriptionChronicleBI;
import org.ihtsdo.otf.tcc.api.description.DescriptionVersionBI;
import org.ihtsdo.otf.tcc.api.lang.LanguageCode;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;
import org.ihtsdo.otf.tcc.api.metadata.binding.SnomedMetadataRf2;
import org.ihtsdo.otf.tcc.api.metadata.binding.TermAux;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import gov.vha.isaac.ochre.api.index.IndexedGenerationCallable;
import gov.vha.isaac.ochre.api.index.SearchResult;

import static org.testng.Assert.*;
import org.ihtsdo.otf.tcc.model.cc.PersistentStore;
import org.ihtsdo.otf.tcc.model.index.service.IndexerBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Class that handles integration tests for <code>Lucene</code> index
 * generation.
 *
 * @author aimeefurber
 */
public class LuceneTest {

    private static final Logger LOGGER = Logger.getLogger(LuceneTest.class.getName());

    static File buildDirFile = null;
    EditCoordinate ec;
    ViewCoordinate vc;

    public LuceneTest() {
    }

    @BeforeMethod
    public void setUp() {
        try {
            int authorNid = TermAux.USER.getLenient().getConceptNid();
            int editPathNid = TermAux.WB_AUX_PATH.getLenient().getConceptNid();

            ec = new EditCoordinate(authorNid, Snomed.CORE_MODULE.getLenient().getNid(), editPathNid);
            vc = PersistentStore.get().getMetadataVC();
        } catch (IOException ex) {
            Logger.getLogger(LuceneTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @AfterMethod
    public void tearDown() {
    }

    @Test(enabled = false)
    public void testLuceneDescriptionIndex() throws IOException, Exception {

        // add test description to concept
        ConceptChronicleBI concept = PersistentStore.get().getConcept(Snomed.BODY_STRUCTURE.getLenient().getNid());
        String testDescription = "Test description lucene index";
        DescriptionCAB descBp = new DescriptionCAB(concept.getPrimordialUuid(),
                SnomedMetadataRf2.SYNONYM_RF2.getLenient().getPrimordialUuid(),
                LanguageCode.EN, testDescription, false, IdDirective.GENERATE_HASH);
        TerminologyBuilderBI builder = PersistentStore.get().getTerminologyBuilder(ec, vc);
        int descNid = descBp.getComponentNid();
        List<IndexerBI> lookers = Hk2Looker.get().getAllServices(IndexerBI.class);
        IndexerBI descriptionIndexer = null;

        for (IndexerBI li : lookers) {
            LOGGER.log(Level.INFO, "Found indexer: {0}", li.getIndexerName());

            if (li.getIndexerName().equals("descriptions")) {
                descriptionIndexer = li;
            }
        }

        assertNotNull(descriptionIndexer);

        IndexedGenerationCallable indexed = descriptionIndexer.getIndexedGenerationCallable(descNid);
        DescriptionChronicleBI newDesc = builder.construct(descBp);

        assertEquals(descNid, newDesc.getNid());
        PersistentStore.get().addUncommitted(concept);
        PersistentStore.get().commit();

        long indexGeneration = indexed.call();

        // search for test description in lucene index
        String[] parts = testDescription.split(" ");
        HashSet<String> wordSet = new HashSet<>();

        for (String word : parts) {
            if (!wordSet.contains(word) && (word.length() > 1) && !word.startsWith("(") && !word.endsWith(")")) {
                word = QueryParser.escape(word);
                wordSet.add(word);
            }
        }

        String queryTerm = null;

        for (String word : wordSet) {
            if (queryTerm == null) {
                queryTerm = "+" + word;
            } else {
                queryTerm = queryTerm + " " + "+" + word;
            }
        }

        List<SearchResult> results = descriptionIndexer.query(queryTerm, ComponentProperty.DESCRIPTION_TEXT, 10000,
                indexGeneration);
        boolean found = false;

        for (SearchResult r : results) {
            if (r.nid == newDesc.getNid()) {
                Optional<DescriptionVersionBI> description
                        = (Optional<DescriptionVersionBI>) PersistentStore.get().getComponentVersion(PersistentStore.get().getMetadataVC(), r.nid);

                if (description.isPresent() && description.get().getText().equals(testDescription)) {
                    found = true;

                    break;
                }
            }
        }

        assertTrue(found);
    }
}
