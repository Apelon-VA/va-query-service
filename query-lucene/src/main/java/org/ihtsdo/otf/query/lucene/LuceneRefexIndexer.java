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
import java.util.logging.Logger;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.TextField;
import org.glassfish.hk2.runlevel.RunLevel;

/**
 *
 * @author kec
 */
@Service(name = "snomed id refex indexer")
@RunLevel(value = 2)
public class LuceneRefexIndexer extends LuceneIndexer {

    private final Logger logger = Logger.getLogger(LuceneRefexIndexer.class.getName());
    int snomedAssemblageSequence = Integer.MIN_VALUE;

    private LuceneRefexIndexer() throws IOException {
        //For HK2
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
    protected boolean indexChronicle(ComponentChronicleBI<?> chronicle) {
        return false;
    }

    @Override
    protected void addFields(ComponentChronicleBI<?> chronicle, Document doc) {
        throw new UnsupportedOperationException("Not supported yet."); 
    }

    @Override
    protected boolean indexSememeChronicle(SememeChronicle<?> chronicle) {
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
    protected void addFields(SememeChronicle<?> chronicle, Document doc) {
        //TODO dan notes - this doesn't make sense - this field should be named REFERENCED_COMPONENT_ID, not COMPONENT_ID
        //The query code makes assumptions about what sort of thing is in the component_id field - we can't have it be component id in one case, 
        //and refereced componentid in another.
        doc.add(new IntField(ComponentProperty.COMPONENT_ID.name(), chronicle.getReferencedComponentNid(),
                    LuceneIndexer.indexedComponentNidType));
        
        for (Object sv: chronicle.getVersions()) {
            if (sv instanceof StringSememe) {
                StringSememe ssv = (StringSememe) sv;
                //TODO this will cause it to only be indexed with the standard analyzer - if we also want to use the whitespace analyzer, 
                //this needs a second document add.  Need to ask Keith about what sort of data will be indexed here.  I suspect, that since we are only 
                //indexing SCTIDs at the moment, that we should only use the whitespace analyzer....
                doc.add(new TextField(ComponentProperty.STRING_EXTENSION_1.name(), ssv.getString(), Field.Store.NO));
            }
        }
    }
}
