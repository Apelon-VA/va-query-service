package org.ihtsdo.otf.query.implementation;

import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.tcc.api.store.Ts;

import javax.xml.bind.annotation.*;
import java.io.IOException;
import java.util.*;

/**
 * Created by kec on 11/2/14.
 */
@XmlRootElement(name = "for-set")
@XmlAccessorType(value = XmlAccessType.NONE)
public class ForSetSpecification {
    @XmlElementWrapper(name = "for")
    @XmlElement(name = "component")
    private List<ComponentCollectionTypes> forCollectionTypes = new ArrayList<>();

    @XmlElementWrapper(name = "custom-for")
    @XmlElement(name = "uuid")
    private Set<UUID> customCollection = new HashSet<>();

    public ForSetSpecification() {
    }

    public ForSetSpecification(ComponentCollectionTypes... forCollectionTypes) {
        this.forCollectionTypes.addAll(Arrays.asList(forCollectionTypes));
    }

    public List<ComponentCollectionTypes> getForCollectionTypes() {
        return forCollectionTypes;
    }

    public void setForCollectionTypes(List<ComponentCollectionTypes> forCollectionTypes) {
        this.forCollectionTypes = forCollectionTypes;
    }

    public Set<UUID> getCustomCollection() {
        return customCollection;
    }

    public void setCustomCollection(Set<UUID> customCollection) {
        this.customCollection = customCollection;
    }

    public NativeIdSetBI getCollection() throws IOException {
        NativeIdSetBI forSet = Ts.get().getEmptyNidSet();
        for (ComponentCollectionTypes collection : forCollectionTypes) {
            switch (collection) {
                case ALL_COMPONENTS:
                    forSet.or(Ts.get().getAllComponentNids());
                    break;
                case ALL_CONCEPTS:
                    forSet.or(Ts.get().getAllConceptNids());
                    break;

                case ALL_DESCRIPTION:
                    forSet.or(Ts.get().getAllComponentNids());
                    break;
                case ALL_RELATIONSHIPS:
                    forSet.or(Ts.get().getAllComponentNids());
                    break;
                case ALL_SEMEMES:
                    forSet.or(Ts.get().getAllComponentNids());
                    break;
                case CUSTOM_SET:
                    for (UUID uuid : customCollection) {
                        forSet.add(Ts.get().getNidForUuids(uuid));
                    }
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
        return forSet;

    }
}
