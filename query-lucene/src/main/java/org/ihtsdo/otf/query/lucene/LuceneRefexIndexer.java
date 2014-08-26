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



package org.ihtsdo.otf.query.lucene;

//~--- non-JDK imports --------------------------------------------------------

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;

import org.ihtsdo.otf.tcc.api.blueprint.ComponentProperty;
import org.ihtsdo.otf.tcc.api.chronicle.ComponentChronicleBI;
import org.ihtsdo.otf.tcc.api.metadata.binding.TermAux;
import org.ihtsdo.otf.tcc.api.refex.RefexChronicleBI;
import org.ihtsdo.otf.tcc.api.refex.type_long.RefexLongVersionBI;
import org.ihtsdo.otf.tcc.model.cc.refex.RefexMember;

import org.ihtsdo.otf.tcc.model.cc.refex.RefexMemberVersion;
import org.jvnet.hk2.annotations.Service;


//~--- JDK imports ------------------------------------------------------------

import java.io.IOException;

import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.ihtsdo.otf.tcc.api.spec.ValidationException;

/**
 *
 * @author kec
 */
@Service
public class LuceneRefexIndexer extends LuceneIndexer {

    int snomedAssemblageNid = Integer.MIN_VALUE;

    public LuceneRefexIndexer() throws IOException {
        super("refex");
    }

    @Override
    protected boolean indexChronicle(ComponentChronicleBI chronicle) {
        if (chronicle instanceof RefexChronicleBI) {
            RefexMember rxc = (RefexMember) chronicle;
            if (snomedAssemblageNid == Integer.MIN_VALUE) {
                try {
                    snomedAssemblageNid = TermAux.SNOMED_IDENTIFIER.getNid();
                } catch (ValidationException ex) {
                    Logger.getLogger(LuceneRefexIndexer.class.getName()).log(Level.SEVERE, null, ex);
                } catch (IOException ex) {
                    Logger.getLogger(LuceneRefexIndexer.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

            if (rxc.getAssemblageNid() == snomedAssemblageNid) {
                return true;
            }
        }

        return false;
    }

    @Override
    protected void addFields(ComponentChronicleBI chronicle, Document doc) {
        RefexMember rxc = (RefexMember) chronicle;
        for (Iterator it = rxc.getVersions().iterator(); it.hasNext(); ) {
            RefexMemberVersion rxv = (RefexMemberVersion) it.next();
            if (rxv instanceof RefexLongVersionBI) {
                RefexLongVersionBI rxvl = (RefexLongVersionBI) rxv;

                doc.add(new LongField(ComponentProperty.LONG_EXTENSION_1.name(), rxvl.getLong1(),
                                      Field.Store.NO));
            }
        }
    }
}
