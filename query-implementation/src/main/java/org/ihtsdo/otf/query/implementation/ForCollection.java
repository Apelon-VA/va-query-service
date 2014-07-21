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
package org.ihtsdo.otf.query.implementation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import org.ihtsdo.otf.tcc.api.nid.ConcurrentBitSet;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.tcc.api.store.TerminologyStoreDI;
import org.ihtsdo.otf.tcc.api.store.Ts;

/**
 * The <code>ForCollection</code> enables the specification of a set of objects
 * over which the query should iterate. The <code>ForCollection</code> can
 * either be specified by an enumeration that identifies a standard set, or by
 * an enumerated list.
 *
 * @author kec
 */
@XmlRootElement(name = "forCollection")
@XmlAccessorType(XmlAccessType.PROPERTY)
public class ForCollection {

    /**
     * An enumeration to specify either a standard set of components, or a
     * custom collection for query iteration.
     */
    public enum ForCollectionContents {

        /**
         * The query should iterate over a collection of all concepts.
         */
        CONCEPT,
        /**
         * The query should iterate over a collection of all components.
         */
        COMPONENT,
        /**
         * The query should iterate over a provided custom collection.
         */
        CUSTOM,
    }
    ForCollectionContents forCollection = ForCollectionContents.CONCEPT;
    List<UUID> customCollection = new ArrayList<>();

    public ForCollection() {
        
    }
    
    /**
     *
     * @param forCollectionSet
     * @return a collection of native identifiers that a query should iterate
     * over.
     * @throws IOException
     */
    public NativeIdSetBI getCollection(String... forCollectionSet) throws IOException {
        TerminologyStoreDI ts = Ts.get();
        switch (forCollection) {
            case COMPONENT:
                return ts.getAllComponentNids();
            case CONCEPT:
                return ts.getAllConceptNids();
            case CUSTOM:
                ConcurrentBitSet cbs = new ConcurrentBitSet();
                for (UUID uuid : customCollection) {
                    cbs.add(ts.getNidForUuids(uuid));
                }
                return cbs;
            default:
                throw new UnsupportedOperationException("Can't handle: " + forCollection);
        }
    }

    /**
     * Return the for collection enumeration as a string.
     *
     * @return
     */
    public String getForCollectionString() {
        return forCollection.name();
    }

    /**
     *
     * @param forCollectionString
     */
    public void setForCollectionString(String forCollectionString) {
        this.forCollection = ForCollectionContents.valueOf(forCollectionString);
    }

    public void setForCollection(ForCollectionContents forCollection) {
        this.forCollection = forCollection;
    }

    /**
     *
     * @return a custom collection of UUIDs over which the query should iterate.
     */
    @XmlAttribute
    public List<UUID> getCustomCollection() {
        return customCollection;
    }

    /**
     *
     * @param customCollection Set the collection of component UUIDs for the
     * query to iterate over.
     */
    public void setCustomCollection(List<UUID> customCollection) {
        this.customCollection = customCollection;
        this.forCollection = ForCollectionContents.CUSTOM;
    }
}
