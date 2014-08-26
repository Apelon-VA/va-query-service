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
package org.ihtsdo.otf.query.rest.server;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.xml.bind.JAXBException;
import org.ihtsdo.otf.query.implementation.JaxbForQuery;
import org.ihtsdo.otf.query.implementation.Query;
import org.ihtsdo.otf.query.implementation.ReturnTypes;
import org.ihtsdo.otf.tcc.api.blueprint.ComponentProperty;
import org.ihtsdo.otf.tcc.api.nid.ConcurrentBitSet;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.tcc.ddo.ResultList;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.ihtsdo.otf.tcc.model.index.service.IndexerBI;
import org.ihtsdo.otf.tcc.model.index.service.SearchResult;
import com.wordnik.swagger.annotations.*;
import org.ihtsdo.otf.query.implementation.versioning.StandardViewCoordinates;

/**
 * Creates a simple REST call for a Lucene search of descriptions. Encode the
 * query string at the following URL: http://www.url-encode-decode.com and
 * select UTF-8.
 *
 * @author dylangrald
 */
@Api(value = "/lucene", description = "Search description text using Lucene.")
@Path("/lucene")
@Produces({"text/plain"})
public class LuceneResource {

    private static IndexerBI descriptionIndexer;

    static {
        List<IndexerBI> lookers = Hk2Looker.get().getAllServices(IndexerBI.class);

        for (IndexerBI li : lookers) {
            System.out.println("AlternativeIdResource found indexer: " + li.getIndexerName());

            if (li.getIndexerName().equals("descriptions")) {
                descriptionIndexer = li;
            }
        }
    }

    @GET
    @Produces("text/plain")
    public String doQuery() throws IOException, JAXBException, Exception {
        return "Put url encoded lucene query at the end of the url";
    }

    @GET
    @Path("/{query}")
    @Produces("text/plain")
    @ApiOperation(value = "Find concepts by description", response = String.class)
    public String doQuery(
            @ApiParam(value = "Search descriptions matching an input string. ", required = true, defaultValue = "hyperphenylalaninemia")
            @PathParam("query") String queryText) throws IOException, JAXBException, Exception {
        String queryString = "query: " + queryText;
        System.out.println("Received: \n   " + queryString);
        if (queryText == null) {
            return "Malformed query. Lucene query must have input query text. \n"
                    + "Found: " + queryString
                    + "\n See: the section on Query Client in the query documentation: \n"
                    + "http://ihtsdo.github.io/OTF-Query-Services/query-documentation/docbook/query-documentation.html";
        }

        //Decode the query text
        queryText = URLDecoder.decode(queryText, "UTF-8");

        try {

            List<SearchResult> results = descriptionIndexer.query(queryText, ComponentProperty.DESCRIPTION_TEXT, 500);

            NativeIdSetBI resultSet = new ConcurrentBitSet();

            System.out.println("result: " + results);
            for (SearchResult r : results) {
                System.out.println("nid: " + r.nid + " score:" + r.score);
            }
            for (SearchResult r : results) {
                resultSet.add(r.nid);
            }
            ArrayList<Object> objectList = Query.returnDisplayObjects(resultSet,
                    ReturnTypes.DESCRIPTION_FOR_COMPONENT,
                    StandardViewCoordinates.getSnomedInferredLatestActiveOnly());

            if (objectList.isEmpty()) {
                return "No results found for " + queryString;
            }

            ResultList resultList = new ResultList();
            resultList.setTheResults(objectList);
            StringWriter writer = new StringWriter();

            JaxbForQuery.get().createMarshaller().marshal(resultList, writer);
            return writer.toString();

        } catch (NullPointerException e) {
            Logger.getLogger(LuceneResource.class.getName()).log(Level.INFO, "Database error.", e);
            throw new QueryApplicationException(HttpErrorType.ERROR503, "Please contact system administrator.");
        }

    }
}
