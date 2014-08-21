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
package org.ihtsdo.otf.query.integration.tests.rest;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.ihtsdo.otf.query.implementation.versioning.StandardViewCoordinates;
import org.ihtsdo.otf.query.integration.tests.QueryTest;
import org.ihtsdo.otf.tcc.api.blueprint.ComponentProperty;
import org.ihtsdo.otf.tcc.api.blueprint.DescriptionCAB;
import org.ihtsdo.otf.tcc.api.blueprint.IdDirective;
import org.ihtsdo.otf.tcc.api.blueprint.InvalidCAB;
import org.ihtsdo.otf.tcc.api.blueprint.RefexCAB;
import org.ihtsdo.otf.tcc.api.blueprint.RefexDirective;
import org.ihtsdo.otf.tcc.api.blueprint.TerminologyBuilderBI;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.api.coordinate.EditCoordinate;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.description.DescriptionChronicleBI;
import org.ihtsdo.otf.tcc.api.description.DescriptionVersionBI;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;
import org.ihtsdo.otf.tcc.api.metadata.binding.TermAux;
import org.ihtsdo.otf.tcc.api.refex.RefexChronicleBI;
import org.ihtsdo.otf.tcc.api.refex.RefexType;
import org.ihtsdo.otf.tcc.model.cc.PersistentStore;

/**
 *
 * @author dylangrald
 */
public class TermstoreChanges {

    public ViewCoordinate vc;

    public TermstoreChanges(ViewCoordinate vc) {
        this.vc = vc;
    }

    public void setActiveStatus(DescriptionVersionBI desc, Status status) throws IOException, ContradictionException, InvalidCAB {
        DescriptionCAB descCAB = desc.makeBlueprint(vc, IdDirective.PRESERVE, RefexDirective.EXCLUDE);
        descCAB.setStatus(status);
        int authorNid = TermAux.USER.getLenient().getConceptNid();
        int editPathNid = TermAux.SNOMED_CORE.getLenient().getConceptNid();
        EditCoordinate ec = new EditCoordinate(authorNid, Snomed.CORE_MODULE.getLenient().getNid(), editPathNid);
        TerminologyBuilderBI tb = PersistentStore.get().getTerminologyBuilder(ec, vc);
        DescriptionChronicleBI descChronicle = tb.construct(descCAB);
        ConceptChronicleBI concept = PersistentStore.get().getConcept(descChronicle.getConceptNid());
        PersistentStore.get().addUncommitted(concept);
        PersistentStore.get().commit();
        System.out.println(descChronicle.getVersion(vc));
    }

    public void addRefsetMember() throws IOException {
        try {
            RefexCAB refex = new RefexCAB(RefexType.STR, Snomed.MILD.getLenient().getNid(), Snomed.SEVERITY_REFSET.getLenient().getNid(), IdDirective.GENERATE_HASH, RefexDirective.INCLUDE);
            refex.put(ComponentProperty.STRING_EXTENSION_1, "Mild severity");
            int authorNid = TermAux.USER.getLenient().getConceptNid();
            int editPathNid = TermAux.WB_AUX_PATH.getLenient().getConceptNid();
            EditCoordinate ec = new EditCoordinate(authorNid, Snomed.CORE_MODULE.getLenient().getNid(), editPathNid);
            TerminologyBuilderBI tb = PersistentStore.get().getTerminologyBuilder(ec, StandardViewCoordinates.getSnomedInferredLatestActiveOnly());
            RefexChronicleBI rc = tb.construct(refex);
            PersistentStore.get().addUncommitted(Snomed.SEVERITY_REFSET.getLenient());
            PersistentStore.get().commit();

        } catch (InvalidCAB | ContradictionException ex) {
            Logger.getLogger(QueryTest.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void modifyDesc(String text, int nid) {
        try {
            DescriptionVersionBI desc = PersistentStore.get().getConceptVersion(vc, nid).getPreferredDescription();
            DescriptionCAB descCAB = desc.makeBlueprint(vc, IdDirective.PRESERVE, RefexDirective.EXCLUDE);
            descCAB.setText(text);
            int authorNid = TermAux.USER.getLenient().getConceptNid();
            int editPathNid = TermAux.SNOMED_CORE.getLenient().getConceptNid();
            EditCoordinate ec = new EditCoordinate(authorNid, Snomed.CORE_MODULE.getLenient().getNid(), editPathNid);
            TerminologyBuilderBI tb = PersistentStore.get().getTerminologyBuilder(ec, vc);
            DescriptionChronicleBI descChronicle = tb.construct(descCAB);
            ConceptChronicleBI concept = PersistentStore.get().getConcept(descChronicle.getConceptNid());
            PersistentStore.get().addUncommitted(concept);
            PersistentStore.get().commit();
        } catch (IOException | ContradictionException | InvalidCAB ex) {
            Logger.getLogger(QueryTest.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }

}
