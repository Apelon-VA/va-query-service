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
package org.ihtsdo.otf.query.rest.client;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import org.ihtsdo.otf.jaxb.chronicle.api.SimpleViewCoordinate;
import org.ihtsdo.otf.jaxb.query.ClauseSemantic;
import org.ihtsdo.otf.jaxb.query.ForCollection;
import org.ihtsdo.otf.jaxb.query.ForCollectionContents;
import org.ihtsdo.otf.jaxb.query.LetMap;
import org.ihtsdo.otf.jaxb.query.ReturnTypes;
import org.ihtsdo.otf.jaxb.query.Where;
import org.ihtsdo.otf.jaxb.query.WhereClause;

/**
 *
 * @author kec
 */
public class QueryProcessorForRestXml {

    //private static final String DEFAULT_HOST = //"http://api.snomedtools.com/otf";
    private static final String DEFAULT_HOST = "http://localhost:8080/otf";
    // Get JAXBContext for converting objects to XML. 
    private static final JAXBContext ctx = JaxbForClient.get();

    public static String process(SimpleViewCoordinate viewpoint,
            ForCollection forObject,
            LetMap letMap,
            Where where,
            ReturnTypes returnType) throws JAXBException, IOException {
        return process(viewpoint, forObject, letMap, where, returnType, DEFAULT_HOST);
    }

    public static String process(SimpleViewCoordinate viewpoint,
            ForCollection forObject,
            LetMap letMap,
            Where where,
            ReturnTypes returnType,
            String host) throws JAXBException, IOException {

        if (viewpoint == null) {
            viewpoint = ViewCoordinateExample.getSnomedInferredLatest();
        }
        if (forObject == null) {
            forObject = new ForCollection();
            forObject.setForCollectionString(ForCollectionContents.CONCEPT.name());
        }
        if (returnType == null) {
            returnType = ReturnTypes.DESCRIPTION_VERSION_FSN;
        }

        // create the client
        Client client = ClientBuilder.newClient();
        // specify the host and the path. 
        WebTarget target = client.target(host).path("query-service/query");

        String viewpointXml = URLEncoder.encode(getXmlString(viewpoint), "UTF-8");
        String forObjectString = URLEncoder.encode(getXmlString(forObject), "UTF-8");
        String letMapString = URLEncoder.encode(getXmlString(letMap), "UTF-8");
        String whereString = URLEncoder.encode(getXmlString(where), "UTF-8");
        String returnTypeString = URLEncoder.encode(getXmlString(returnType), "UTF-8");

        StringBuilder bi = new StringBuilder();
        bi.append("VIEWPOINT=").append(viewpointXml).append("&FOR=").append(forObjectString).append("&LET=").append(letMapString).append("&WHERE=").append(whereString).append("&RETURN=").append(returnTypeString);

        System.out.println("Query URL: ");
        System.out.println(bi.toString());

        return target.queryParam("VIEWPOINT", getXmlString(viewpoint)).
                queryParam("FOR", getXmlString(forObject)).
                queryParam("LET", getXmlString(letMap)).
                queryParam("WHERE", getXmlString(where)).
                queryParam("RETURN", getXmlString(returnType)).
                request(MediaType.TEXT_PLAIN).get(String.class);

    }

    public static String lucene(String queryText) throws UnsupportedEncodingException, JAXBException {
        return lucene(queryText, DEFAULT_HOST);
    }

    public static String lucene(String queryText, String host) throws UnsupportedEncodingException, JAXBException {
        Client client = ClientBuilder.newClient();

        WebTarget target = client.target(host).path("query-service/lucene/");

        String encodedUrl = URLEncoder.encode(queryText, "UTF-8");

        return target.queryParam("Query string", encodedUrl)
                .request(MediaType.TEXT_PLAIN).get(String.class);
    }

    public static String regex(String queryText) throws UnsupportedEncodingException, JAXBException {
        return regex(queryText, DEFAULT_HOST);
    }

    public static String regex(String queryText, String host) throws UnsupportedEncodingException, JAXBException {
        Client client = ClientBuilder.newClient();

        WebTarget target = client.target(host).path("query-service/regex/");

        String encodedUrl = URLEncoder.encode(queryText, "UTF-8");

        return target.queryParam("Query string", encodedUrl)
                .request(MediaType.TEXT_PLAIN).get(String.class);
    }

    private static String getXmlString(Object obj) throws JAXBException {
        if (obj instanceof SimpleViewCoordinate) {
            org.ihtsdo.otf.jaxb.chronicle.api.ObjectFactory factory = new org.ihtsdo.otf.jaxb.chronicle.api.ObjectFactory();
            obj = factory.createSimpleViewCoordinate((SimpleViewCoordinate) obj);

        } else if (obj instanceof ForCollection) {
            org.ihtsdo.otf.jaxb.query.ObjectFactory factory = new org.ihtsdo.otf.jaxb.query.ObjectFactory();
            obj = factory.createForCollection((ForCollection) obj);
        } else if (obj instanceof LetMap) {
            org.ihtsdo.otf.jaxb.query.ObjectFactory factory = new org.ihtsdo.otf.jaxb.query.ObjectFactory();
            obj = factory.createLetMap((LetMap) obj);
        } else if (obj instanceof Where) {
            org.ihtsdo.otf.jaxb.query.ObjectFactory factory = new org.ihtsdo.otf.jaxb.query.ObjectFactory();
            obj = factory.createWhere((Where) obj);
        } else if (obj instanceof ReturnTypes) {
            org.ihtsdo.otf.jaxb.query.ObjectFactory factory = new org.ihtsdo.otf.jaxb.query.ObjectFactory();
            obj = factory.createReturnTypes((ReturnTypes) obj);
        }
        StringWriter writer = new StringWriter();
        ctx.createMarshaller().marshal(obj, writer);
        return writer.toString();
    }
}
