package gov.vha.isaac.cradle.component;

import gov.vha.isaac.cradle.CradleExtensions;
import org.ihtsdo.otf.tcc.api.chronicle.ComponentChronicleBI;
import org.ihtsdo.otf.tcc.api.nid.NidListBI;
import org.ihtsdo.otf.tcc.api.nid.NidSetBI;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.ihtsdo.otf.tcc.model.cc.attributes.ConceptAttributes;
import org.ihtsdo.otf.tcc.model.cc.component.ConceptComponent;
import org.ihtsdo.otf.tcc.model.cc.concept.I_ManageConceptData;
import org.ihtsdo.otf.tcc.model.cc.description.Description;
import org.ihtsdo.otf.tcc.model.cc.media.Media;
import org.ihtsdo.otf.tcc.model.cc.refex.RefexMember;
import org.ihtsdo.otf.tcc.model.cc.refexDynamic.RefexDynamicMember;
import org.ihtsdo.otf.tcc.model.cc.relationship.Relationship;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;
import org.ihtsdo.otf.tcc.api.store.Ts;
import org.ihtsdo.otf.tcc.model.cc.concept.ConceptChronicle;

/**
 * Created by kec on 7/19/14.
 */
public class ConceptChronicleDataEager implements I_ManageConceptData {

    public static short CURRENT_FORMAT_VERSION = 1;

    protected ConceptAttributes attributes;
    protected List<Description> descriptions = new ArrayList<>();
    protected List<Relationship> relationships = new ArrayList<>();
    protected List<Media> media = new ArrayList<>();
    protected List<RefexDynamicMember> refexDynamicMembers = new ArrayList<>();

    private boolean conceptForgotten = false;
    private boolean primordial;
    private boolean annotationStyleRefex;

    public ConceptChronicleDataEager(boolean primordial) {
        this.primordial = primordial;
    }
    
    public Stream<ConceptComponent<?,?>> getConceptComponents() {
        Stream.Builder<ConceptComponent<?,?>> builder = Stream.builder();
        if (attributes != null) {
            builder.accept(attributes);
        }
        descriptions.stream().forEach((Description d) -> {
            builder.accept(d);
        });
        relationships.stream().forEach((Relationship r) -> {
            builder.accept(r);
        });
        media.stream().forEach((Media m) -> {
            builder.accept(m);
        });
        refexDynamicMembers.stream().forEach((RefexDynamicMember m) -> {
            builder.accept(m);
        });
        return builder.build();
    }

    @Override
    public void forgetConcept() {
        conceptForgotten = true;
    }

    @Override
    public boolean isConceptForgotten() {
        return conceptForgotten;
    }

    @Override
    public List<Description> getDescriptions() throws IOException {
        return descriptions;
    }

    @Override
    public void add(Description desc) throws IOException {
        this.descriptions.add(desc);
    }

    @Override
    public void add(Media media) throws IOException {
        this.media.add(media);
    }

    private static CradleExtensions isaacDb;

    private static CradleExtensions getIsaacDb() {
        if (isaacDb == null) {
            isaacDb = Hk2Looker.getService(CradleExtensions.class);
        }
        return isaacDb;
    }

    @Override
    public void add(RefexMember<?, ?> refexMember) throws IOException {
        getIsaacDb().writeSememe(refexMember);
    }

    /**
     * TODO change to get Sememe
     *
     * @param nid of the Sememe
     * @return
     * @throws IOException
     */
    @Override
    public RefexMember<?, ?> getRefsetMember(int nid) throws IOException {

        return (RefexMember<?, ?>) getIsaacDb().getSememe(nid);
    }

    /**
     * TODO change to get Sememe TODO consider changing to get a Collection
     * instead of an individual. TODO consider having a different method for
     * getting a Collection instead of an individual.
     *
     * @param componentNid
     * @return
     * @throws IOException
     */
    @Override
    public RefexMember<?, ?> getRefsetMemberForComponent(int componentNid) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<RefexMember<?, ?>> getRefsetMembers() throws IOException {
        return (Collection<RefexMember<?, ?>>) Ts.get().getSememesForAssemblage(getNid());
    }

    @Override
    public void add(RefexDynamicMember refexDynamicMember) throws IOException {
        this.refexDynamicMembers.add(refexDynamicMember);
    }

    @Override
    public void add(Relationship rel) throws IOException {
        this.relationships.add(rel);
    }

    @Override
    public void cancel() throws IOException {

    }

    @Override
    public void modified(ComponentChronicleBI component) {

    }

    @Override
    public void modified(ConceptComponent component, long sequence) {

    }

    @Override
    public boolean readyToWrite() {
        return false;
    }

    @Override
    public Collection<Integer> getAllNids() throws IOException {
        return null;
    }

    @Override
    public ComponentChronicleBI<?> getComponent(int nid) throws IOException {
        Optional<ComponentChronicleBI<?>> result = getComponent(nid, attributes);
        if (result.isPresent()) {
            return result.get();
        }
        result = getComponent(nid, descriptions);
        if (result.isPresent()) {
            return result.get();
        }
        result = getComponent(nid, media);
        if (result.isPresent()) {
            return result.get();
        }
        result = getComponent(nid, relationships);
        if (result.isPresent()) {
            return result.get();
        }
        result = getComponent(nid, getRefsetMembers());
        if (result.isPresent()) {
            return result.get();
        }
        result = getComponent(nid, getRefsetDynamicMembers());
        if (result.isPresent()) {
            return result.get();
        }
        return null;
    }

    public Optional<ComponentChronicleBI<?>> getComponent(int nid, Collection<? extends ComponentChronicleBI<?>> componentsToSearch) {
        for (ComponentChronicleBI<?> component : componentsToSearch) {
            if (component.getNid() == nid) {
                return Optional.of(component);
            }
        }
        return Optional.empty();
    }

    public Optional<ComponentChronicleBI<?>> getComponent(int nid, ComponentChronicleBI<?> componentToSearch) {
        if (componentToSearch.getNid() == nid) {
            return Optional.of(componentToSearch);
        }
        return Optional.empty();
    }

    @Override
    public ConceptAttributes getConceptAttributes() throws IOException {
        return this.attributes;
    }

    @Override
    public Collection<Integer> getConceptNidsAffectedByCommit() throws IOException {
        return null;
    }

    /**
     * TODO this is used in the merging process. Need to see if we can eliminate
     * use of this method from the merge process...
     *
     * @return
     * @throws IOException
     */
    @Override
    public Set<Integer> getDescNids() throws IOException {
        return getNidSet(getDescriptions());
    }

    private Set<Integer> getNidSet(Collection<? extends ConceptComponent> components) throws IOException {
        HashSet<Integer> nids = new HashSet<>();
            components.stream().forEach((component) -> {
                nids.add(component.getNid());
            });
         return nids;
    }

    @Override
    public List<Relationship> getDestRels() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Relationship> getDestRels(NidSetBI allowedTypes) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Integer> getImageNids() throws IOException {
        return getNidSet(getMedia());
    }

    @Override
    public List<Media> getMedia() throws IOException {
        return this.media;
    }

    @Override
    public Set<Integer> getMemberNids() throws IOException {
        return getNidSet(getRefsetMembers());
    }

    @Override
    public int getNid() {
        return this.attributes.getNid();
    }

    @Override
    public RefexDynamicMember getRefsetDynamicMember(int memberNid) throws IOException {
        return null;
    }

    @Override
    public Collection<RefexDynamicMember> getRefsetDynamicMembers() throws IOException {
        return this.refexDynamicMembers;
    }

    @Override
    public Collection<Relationship> getSourceRels() throws IOException {
        return this.relationships;
    }

    @Override
    public Set<Integer> getSrcRelNids() throws IOException {
        return getNidSet(getSourceRels());
    }

    @Override
    public NidListBI getUncommittedNids() {
        return null;
    }

    @Override
    public boolean isAnnotationStyleRefex() throws IOException {
        return this.annotationStyleRefex;
    }

    @Override
    public boolean isPrimordial() throws IOException {
        return primordial;
    }

    @Override
    public boolean isUncommitted() {
        return false;
    }

    @Override
    public boolean isUnwritten() {
        return false;
    }

    @Override
    public void setConceptAttributes(ConceptAttributes attr) throws IOException {
        this.attributes = attr;
    }

    @Override
    public void setDescriptions(Set<Description> descriptions) throws IOException {
        this.descriptions.clear();
        this.descriptions.addAll(descriptions);
    }

    @Override
    public void setSourceRels(Set<Relationship> relationships) throws IOException {
        this.relationships.clear();
        this.relationships.addAll(relationships);
    }

    @Override
    public void setIsAnnotationStyleRefex(boolean annotationStyleRefex) {
        this.annotationStyleRefex = annotationStyleRefex;
    }

    @Override
    public void setPrimordial(boolean isPrimordial) {
        this.primordial = isPrimordial;
    }

    @Override
    public NidSetBI setCommitTime(long time) {
        return null;
    }

    @Override
    public ConceptChronicle getConceptChronicle() {
        try {
            return ConceptChronicle.get(this.getNid(), this);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public String toString() {
        return "ConceptChronicleDataEager{" + "attributes=" + attributes + ", descriptions=" + descriptions + ", relationships=" + relationships + ", media=" + media + ", refexDynamicMembers=" + refexDynamicMembers + ", conceptForgotten=" + conceptForgotten + ", primordial=" + primordial + ", annotationStyleRefex=" + annotationStyleRefex + '}';
    }
}
