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

import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.sememe.SememeChronicle;
import gov.vha.isaac.ochre.api.sememe.SememeType;
import gov.vha.isaac.ochre.api.sememe.version.StringSememe;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import org.ihtsdo.otf.tcc.api.blueprint.ComponentProperty;
import org.ihtsdo.otf.tcc.api.chronicle.ComponentChronicleBI;
import org.jvnet.hk2.annotations.Service;


//~--- JDK imports ------------------------------------------------------------

import java.io.IOException;
import java.util.logging.Level;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.TextField;
import org.glassfish.hk2.runlevel.RunLevel;
import static org.ihtsdo.otf.query.lucene.LuceneIndexer.logger;

/**
 *
 * @author kec
 */
@Service(name = "snomed id refex indexer")
@RunLevel(value = 2)
public class LuceneRefexIndexer extends LuceneIndexer {

    int snomedAssemblageSequence = Integer.MIN_VALUE;

    public LuceneRefexIndexer() throws IOException {
        super("refex");
    }
    
    @PostConstruct
    private void startMe() throws IOException {
        logger.info("Starting LuceneRefexIndexer post-construct");
        
    }
    
    @PreDestroy
    private void stopMe() throws IOException {
        logger.info("Stopping LuceneRefexIndexer pre-destroy. ");
        commitWriter();
        closeWriter();
    }

    @Override
    protected boolean indexChronicle(ComponentChronicleBI chronicle) {
        return false;
    }

    @Override
    protected void addFields(ComponentChronicleBI chronicle, Document doc) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected boolean indexSememeChronicle(SememeChronicle chronicle) {
        if (chronicle.getSememeType() == SememeType.STRING) {
            if (snomedAssemblageSequence == Integer.MIN_VALUE) {
                snomedAssemblageSequence = IsaacMetadataAuxiliaryBinding.SNOMED_INTEGER_ID.getSequence();
            }

            if (chronicle.getAssemblageSequence() == snomedAssemblageSequence) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void addFields(SememeChronicle chronicle, Document doc) {
        doc.add(new IntField(ComponentProperty.COMPONENT_ID.name(), chronicle.getReferencedComponentNid(),
                    LuceneIndexer.indexedComponentNidType));
        
        for (Object sv: chronicle.getVersions()) {
            if (sv instanceof StringSememe) {
                StringSememe ssv = (StringSememe) sv;
                doc.add(new TextField(ComponentProperty.STRING_EXTENSION_1.name(), ssv.getString(),
                                      Field.Store.NO));
            }
        }
    }
}
