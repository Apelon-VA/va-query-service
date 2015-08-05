package gov.vha.isaac.cradle.component;

import gov.vha.isaac.cradle.collections.CradleSerializer;
import org.ihtsdo.otf.tcc.model.cc.attributes.ConceptAttributes;
import org.ihtsdo.otf.tcc.model.cc.attributes.ConceptAttributesSerializer;
import org.ihtsdo.otf.tcc.model.cc.component.ArrayListCollector;
import org.ihtsdo.otf.tcc.model.cc.description.Description;
import org.ihtsdo.otf.tcc.model.cc.description.DescriptionSerializer;
import org.ihtsdo.otf.tcc.model.cc.media.Media;
import org.ihtsdo.otf.tcc.model.cc.media.MediaSerializer;
import org.ihtsdo.otf.tcc.model.cc.refexDynamic.RefexDynamicMember;
import org.ihtsdo.otf.tcc.model.cc.relationship.Relationship;
import org.ihtsdo.otf.tcc.model.cc.relationship.RelationshipSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by kec on 7/19/14.
 */
public class ConceptChronicleDataEagerSerializer implements CradleSerializer<ConceptChronicleDataEager>, Serializable {


    @Override
    public void serialize(DataOutput dataOutput, ConceptChronicleDataEager conceptChronicleData) {

        try {
            ConceptAttributesSerializer.get().serialize(dataOutput, conceptChronicleData.getConceptAttributes());
            DescriptionSerializer.get().serialize(dataOutput,conceptChronicleData.getDescriptions());
            RelationshipSerializer.get().serialize(dataOutput,conceptChronicleData.getSourceRels());
            MediaSerializer.get().serialize(dataOutput,conceptChronicleData.getMedia());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public ConceptChronicleDataEager deserialize(DataInput dataInput) {
        try {
            ConceptChronicleDataEager conceptChronicleData = new ConceptChronicleDataEager(false);
            
            ConceptAttributes attributes = new ConceptAttributes();
            attributes.setModificationTracker(conceptChronicleData);
            conceptChronicleData.attributes = ConceptAttributesSerializer.get().deserialize(dataInput, attributes);
            
            ArrayListCollector<Description> descriptionArrayListCollector = new ArrayListCollector<>();
            DescriptionSerializer.get().deserialize(dataInput, descriptionArrayListCollector, conceptChronicleData);
            conceptChronicleData.descriptions = new CopyOnWriteArrayList<>(descriptionArrayListCollector.getCollection());
            
            ArrayListCollector<Relationship> relationshipArrayListCollector = new ArrayListCollector<>();
            RelationshipSerializer.get().deserialize(dataInput, relationshipArrayListCollector, conceptChronicleData);
            conceptChronicleData.relationships = new CopyOnWriteArrayList<>(relationshipArrayListCollector.getCollection());
            
            ArrayListCollector<Media> mediaArrayListCollector = new ArrayListCollector<>();
            MediaSerializer.get().deserialize(dataInput, mediaArrayListCollector, conceptChronicleData);
            conceptChronicleData.media = new CopyOnWriteArrayList<>(mediaArrayListCollector.getCollection());
            
            
            return conceptChronicleData;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
     }

}
