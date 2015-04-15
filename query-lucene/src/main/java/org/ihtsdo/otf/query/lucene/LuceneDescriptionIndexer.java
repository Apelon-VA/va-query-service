package org.ihtsdo.otf.query.lucene;

//~--- non-JDK imports --------------------------------------------------------

import gov.vha.isaac.ochre.api.sememe.SememeChronicle;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;

import org.ihtsdo.otf.tcc.api.chronicle.ComponentChronicleBI;
import org.ihtsdo.otf.tcc.api.description.DescriptionChronicleBI;
import org.ihtsdo.otf.tcc.api.description.DescriptionVersionBI;
import org.ihtsdo.otf.tcc.model.index.service.IndexerBI;

import org.jvnet.hk2.annotations.Service;

//~--- JDK imports ------------------------------------------------------------

import java.io.IOException;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.glassfish.hk2.runlevel.RunLevel;
import org.ihtsdo.otf.tcc.api.blueprint.ComponentProperty;

/**
 * Lucene Manager for a Description index. Provides the description indexing
 * service.
 *
 * @author aimeefurber
 */
@Service(name = "Description indexer")
@RunLevel(value = 2)
public class LuceneDescriptionIndexer extends LuceneIndexer implements IndexerBI {
    public LuceneDescriptionIndexer() throws IOException {
        super("descriptions");
    }

    @Override
    protected boolean indexChronicle(ComponentChronicleBI chronicle) {
        if (chronicle instanceof DescriptionChronicleBI) {
            return true;
        }

        return false;
    }

    @Override
    protected void addFields(ComponentChronicleBI chronicle, Document doc) {
        if (chronicle instanceof DescriptionChronicleBI) {
            DescriptionChronicleBI desc = (DescriptionChronicleBI) chronicle;
            String lastDescText = null;

            for (DescriptionVersionBI descriptionVersion : desc.getVersions()) {
                if ((lastDescText == null) || 
                        (lastDescText.equals(descriptionVersion.getText()) == false)) {
                    doc.add(new TextField(ComponentProperty.DESCRIPTION_TEXT.name(), 
                            descriptionVersion.getText(), Field.Store.NO));
                    lastDescText = descriptionVersion.getText();
                } 
            }
        }
    }
    
    @PostConstruct
    private void startMe() throws IOException {
        logger.info("Starting LuceneDescriptionIndexer post-construct");
        
    }
    
    @PreDestroy
    private void stopMe() throws IOException {
        logger.info("Stopping LuceneDescriptionIndexer pre-destroy. ");
        commitWriter();
        closeWriter();
    }

    @Override
    protected boolean indexSememeChronicle(SememeChronicle chronicle) {
        return false;
    }

    @Override
    protected void addFields(SememeChronicle chronicle, Document doc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }


}
