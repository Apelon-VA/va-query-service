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

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import org.ihtsdo.otf.query.implementation.QueryFromJaxb;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import org.ihtsdo.otf.query.implementation.JaxbForQuery;
import org.ihtsdo.otf.query.implementation.ReturnTypes;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.tcc.api.spec.ValidationException;
import org.ihtsdo.otf.tcc.ddo.ResultList;

/**
 * Perform a query and return results.
 *
 * @author kec
 */
@Api(value = "/query", description = "Retrieve components based upon query criterion.")
@Path("/query")
@Produces({"text/plain"})
public class QueryResource {

    @GET
    @Produces("text/plain")
    @ApiOperation(value = "Find results from the LET and WHERE objects and VIEWPOINT, FOR, and RETURN values.", response = String.class)
    @ApiResponses(value = {
        @ApiResponse(code = 422, message = "Invalid input objects"),
        @ApiResponse(code = 414, message = "Request-URI Too Long.")})
    public String doQuery(
            @ApiParam(value = "Version of SNOMED to query.", required = true, defaultValue = "null")
            @QueryParam("VIEWPOINT") String viewValue,
            @ApiParam(value = "Components to iterate over.", required = true, defaultValue = "null")
            @QueryParam("FOR") String forValue,
            @ApiParam(value = "Map used for objects required for Where clause", required = true, defaultValue ="<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><ns2:letMap xmlns:ns2=\"http://query.jaxb.otf.ihtsdo.org\"><map><entry><key>allergic-asthma</key><value xsi:type=\"ns4:simpleConceptSpecification\" xmlns:ns4=\"http://api.chronicle.jaxb.otf.ihtsdo.org\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><description>Allergic asthma</description><uuid>531abe20-8324-3db9-9104-8bcdbf251ac7</uuid></value></entry><entry><key>Is a</key><value xsi:type=\"ns4:simpleConceptSpecification\" xmlns:ns4=\"http://api.chronicle.jaxb.otf.ihtsdo.org\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><description>Is a (attribute)</description><uuid>c93a30b9-ba77-3adb-a9b8-4589c9f8fb25</uuid></value></entry></map></ns2:letMap>")
            @QueryParam("LET") String letValue,
            @ApiParam(value = "Clauses used to create criterion to find components.", required = true, defaultValue="<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><ns2:where xmlns:ns2=\"http://query.jaxb.otf.ihtsdo.org\"><rootClause><letKeys>Is a</letKeys><letKeys>allergic-asthma</letKeys><semanticString>REL_TYPE</semanticString></rootClause></ns2:where>")
            @QueryParam("WHERE") String whereValue,
            @ApiParam(value = "Information returned from result components. Default is fully specified description version.", required = true, defaultValue = "null")
            @QueryParam("RETURN") String returnValue) throws IOException, JAXBException, Exception {
        String queryString = "VIEWPOINT: " + viewValue + "\n   "
                + "FOR: " + forValue + "\n   "
                + "LET: " + letValue + "\n   "
                + "WHERE: " + whereValue + "\n   "
                + "RETURN: " + returnValue;
        System.out.println("Received: \n   " + queryString);

        if (letValue == null && whereValue == null) {
            return ("Enter the required LET and WHERE parameters. See the documentation at "
                    + "http://ihtsdo.github.io/OTF-Query-Services/query-documentation/docbook/query-documentation.html for more information.");
        }

        QueryFromJaxb query;
        try {
            query = new QueryFromJaxb(viewValue, forValue, letValue, whereValue);

        } catch (NullPointerException e) {
            Logger.getLogger(QueryResource.class
                    .getName()).log(Level.INFO, "Database error.", e);
            throw new QueryApplicationException(HttpErrorType.ERROR503,
                    "Please contact system administrator.");
        }

        try {
            query.getViewCoordinate();
        } catch (NullPointerException e) {
            throw new QueryApplicationException(HttpErrorType.ERROR422, "Malformed VIEWPOINT value.");
        }

        try {
            query.getForCollection();
        } catch (NullPointerException e) {
            throw new QueryApplicationException(HttpErrorType.ERROR422, "Malformed FOR value.");
        }

        try {
            query.getLetDeclarations();
        } catch (NullPointerException e) {
            throw new QueryApplicationException(HttpErrorType.ERROR422, "Malformed LET value.");
        }

        try {
            query.getRootClause();
        } catch (NullPointerException e) {
            throw new QueryApplicationException(HttpErrorType.ERROR422, "Malformed WHERE value.");
        }

        if (query.getViewCoordinate() == null) {
            throw new QueryApplicationException(HttpErrorType.ERROR422, "Malformed VIEWPOINT value.");
        } else if (query.getForCollection() == null) {
            throw new QueryApplicationException(HttpErrorType.ERROR422, "Malformed FOR value.");
        } else if (query.getLetDeclarations() == null) {
            throw new QueryApplicationException(HttpErrorType.ERROR422, "Malformed LET value.");
        } else if (query.getRootClause() == null) {
            throw new QueryApplicationException(HttpErrorType.ERROR422, "Malformed WHERE value.");
        } else if (query.nullSpec == true) {
            throw new QueryApplicationException(HttpErrorType.ERROR422, "Null ConceptSpec.");
        }

        NativeIdSetBI resultSet = null;

        try {
            resultSet = query.compute();
        } catch (ValidationException e) {
            throw new QueryApplicationException(HttpErrorType.ERROR422, "Malformed input concept in LET value. See below for ValidationException details.", e);
        }

        if (!returnValue.equals(
                "null") && !returnValue.equals("")) {
            ReturnTypes returnType;
            if (returnValue.startsWith("<?xml")) {
                try {
                    Unmarshaller unmarshaller = JaxbForQuery.get().createUnmarshaller();
                    returnType = (ReturnTypes) unmarshaller.unmarshal(new StringReader(returnValue));
                } catch (JAXBException e) {
                    throw new QueryApplicationException(HttpErrorType.ERROR422, "Malformed RETURN value.");
                }
            } else {
                try {
                    returnType = ReturnTypes.valueOf(returnValue);
                } catch (IllegalArgumentException e) {
                    throw new QueryApplicationException(HttpErrorType.ERROR422, "Malformed RETURN value.");
                }
            }
            ArrayList<Object> objectList = query.returnDisplayObjects(resultSet,
                    returnType);

            ResultList resultList = new ResultList();
            resultList.setTheResults(objectList);
            StringWriter writer = new StringWriter();

            JaxbForQuery.get().createMarshaller().marshal(resultList, writer);
            return writer.toString();
        } else {
            //The default return type is DESCRIPTION_VERSION_FSN
            ArrayList<Object> objectList = query.returnDisplayObjects(resultSet, ReturnTypes.DESCRIPTION_VERSION_FSN);

            ResultList resultList = new ResultList();
            resultList.setTheResults(objectList);
            StringWriter writer = new StringWriter();

            JaxbForQuery.get().createMarshaller().marshal(resultList, writer);
            return writer.toString();
        }
    }
}
