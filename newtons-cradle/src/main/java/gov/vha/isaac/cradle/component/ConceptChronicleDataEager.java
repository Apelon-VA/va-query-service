package gov.vha.isaac.cradle.component;

import gov.vha.isaac.cradle.CradleExtensions;
import gov.vha.isaac.ochre.api.Get;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
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
import org.ihtsdo.otf.tcc.model.cc.relationship.Relationship;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.ihtsdo.otf.tcc.api.store.Ts;
import org.ihtsdo.otf.tcc.model.cc.concept.ConceptChronicle;

/**
 * Created by kec on 7/19/14.
 */
public class ConceptChronicleDataEager implements I_ManageConceptData {

    private static CradleExtensions cradle;

    private static CradleExtensions getCradle() {
        if (cradle == null) {
            cradle = Hk2Looker.getService(CradleExtensions.class);
        }
        return cradle;
    }

    public static short CURRENT_FORMAT_VERSION = 1;

    protected ConceptAttributes attributes;
    protected List<Description> descriptions = new ArrayList<>();
    protected List<Relationship> relationships = new ArrayList<>();
    protected List<Media> media = new ArrayList<>();

    private boolean conceptForgotten = false;
    private boolean primordial;
    private boolean annotationStyleRefex;
    private long lastModified = Long.MIN_VALUE;

    public ConceptChronicleDataEager(boolean primordial) {
        this.primordial = primordial;
    }

    public Stream<ConceptComponent<?, ?>> getConceptComponents() {
        Stream.Builder<ConceptComponent<?, ?>> builder = Stream.builder();
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
    public List<Description> getDescriptions() {
        return descriptions;
    }

    @Override
    public void add(Description desc) {
        this.descriptions.add(desc);
    }

    @Override
    public void add(Media media) {
        this.media.add(media);
    }

    @Override
    public void add(RefexMember<?, ?> refexMember) {
        getCradle().writeRefex(refexMember);
    }

    /**
     * TODO change to get Sememe
     *
     * @param nid of the Sememe
     * @return
     */
    @Override
    public RefexMember<?, ?> getRefsetMember(int nid) {

        return (RefexMember<?, ?>) getCradle().getRefex(nid);
    }

    /**
     * TODO change to get Sememe TODO consider changing to get a Collection
     * instead of an individual. TODO consider having a different method for
     * getting a Collection instead of an individual.
     *
     * @param componentNid
     * @return
     */
    @Override
    public RefexMember<?, ?> getRefsetMemberForComponent(int componentNid) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<RefexMember<?, ?>> getRefsetMembers() {
        return getRefsetMembers(getNid());
    }

    private Collection<RefexMember<?, ?>> getRefsetMembers(int nid) {
        return (Collection<RefexMember<?, ?>>) Ts.get().getRefexesForAssemblage(nid);
    }

    @Override
    public void add(Relationship rel) {
        this.relationships.add(rel);
    }

    @Override
    public void cancel() throws IOException {

    }

    @Override
    public void modified(ComponentChronicleBI component) {
        lastModified = getCradle().incrementAndGetSequence();
    }

    @Override
    public void modified(ConceptComponent component, long sequence) {
        lastModified = sequence;
    }

    @Override
    public boolean readyToWrite() {
        return true;
    }

    @Override
    public Collection<Integer> getAllNids() {
        return getConceptComponents().mapToInt((ConceptComponent<?, ?> component)
                -> component.getNid()).boxed().collect(Collectors.toList());
    }

    @Override
    public ComponentChronicleBI<?> getComponent(int nid) {
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
        if (componentToSearch == null) {
            return Optional.empty();
        }
        if (componentToSearch.getNid() == nid) {
            return Optional.of(componentToSearch);
        }
        return Optional.empty();
    }

    @Override
    public ConceptAttributes getConceptAttributes() {
        return this.attributes;
    }

    @Override
    public Collection<Integer> getConceptNidsAffectedByCommit() {
        return null;
    }

    /**
     * TODO this is used in the merging process. Need to see if we can eliminate
     * use of this method from the merge process...
     *
     * @return
     */
    @Override
    public Set<Integer> getDescNids() {
        return getNidSet(getDescriptions());
    }

    private Set<Integer> getNidSet(Collection<? extends ConceptComponent> components) {
        HashSet<Integer> nids = new HashSet<>();
        components.stream().forEach((component) -> {
            nids.add(component.getNid());
        });
        return nids;
    }

    @Override
    public List<Relationship> getDestRels() {
        ConceptSequenceSet relOriginConceptSequences
                = ConceptSequenceSet.of(Get.taxonomyService().getAllRelationshipOriginSequences(this.getNid()));

        return getCradle().getConceptDataEagerStream(relOriginConceptSequences)
                .map(ConceptChronicleDataEager::getSourceRels)
                .flatMap(Collection::stream)
                .filter((relationship) -> {
                    return relationship.getDestinationNid() == getNid();
                })
                .collect(Collectors.toList());

    }

    @Override
    public List<Relationship> getDestRels(NidSetBI allowedTypes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Integer> getImageNids() {
        return getNidSet(getMedia());
    }

    @Override
    public List<Media> getMedia() {
        return this.media;
    }

    @Override
    public Set<Integer> getMemberNids() {
        return getNidSet(getRefsetMembers());
    }

    @Override
    public int getNid() {
        return attributes == null ? Integer.MAX_VALUE : this.attributes.getNid();
    }

    @Override
    public Collection<Relationship> getSourceRels() {
        return this.relationships;
    }

    @Override
    public Set<Integer> getSrcRelNids() {
        return getNidSet(getSourceRels());
    }

    @Override
    public NidListBI getUncommittedNids() {
        return null;
    }

    @Override
    public boolean isAnnotationStyleRefex() {
        return this.annotationStyleRefex;
    }

    @Override
    public boolean isPrimordial() {
        return primordial;
    }

    @Override
    public boolean isUncommitted() {

        return getConceptComponents().anyMatch((ConceptComponent<?, ?> component) -> {
            return component.isUncommitted();
        });
    }

    @Override
    public boolean isUnwritten() {
        return false;
    }

    @Override
    public void setConceptAttributes(ConceptAttributes attr) {
        this.attributes = attr;
    }

    @Override
    public void setDescriptions(Set<Description> descriptions) {
        this.descriptions.clear();
        this.descriptions.addAll(descriptions);
    }

    @Override
    public void setSourceRels(Set<Relationship> relationships) {
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
        return "ConceptChronicleDataEager{" + "attributes=" + attributes + ", descriptions=" + descriptions + ", relationships=" + relationships + ", media=" + media +  ", conceptForgotten=" + conceptForgotten + ", primordial=" + primordial + ", annotationStyleRefex=" + annotationStyleRefex + '}';
    }
}
