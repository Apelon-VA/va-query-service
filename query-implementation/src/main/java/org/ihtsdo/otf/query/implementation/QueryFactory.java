package org.ihtsdo.otf.query.implementation;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.IOException;

/**
 * Created by kec on 11/4/14.
 */
public class QueryFactory {


    public static Query createQuery() {
        return new QueryFromFactory();
    }

    @XmlRootElement(name = "query")
    public static class QueryFromFactory extends Query {
        @Override
        protected ForSetSpecification ForSetSpecification() throws IOException {
            ForSetSpecification forSetSpec = new ForSetSpecification();
            forSetSpec.setForCollectionTypes(forCollectionTypes);
            forSetSpec.setCustomCollection(customCollection);
            return forSetSpec;
        }

        @Override
        public void Let() throws IOException {
            // Set directly by Jaxb
        }

        @Override
        public Clause Where() {
            return rootClause[0];
        }
    }

}
