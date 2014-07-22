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
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.xml.bind.JAXBException;
import org.ihtsdo.otf.query.implementation.JaxbForQuery;
import org.ihtsdo.otf.tcc.api.blueprint.ComponentProperty;
import org.ihtsdo.otf.tcc.api.chronicle.ComponentChronicleBI;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.api.concept.ConceptVersionBI;
import org.ihtsdo.otf.tcc.api.coordinate.StandardViewCoordinates;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.description.DescriptionChronicleBI;
import org.ihtsdo.otf.tcc.api.description.DescriptionVersionBI;
import org.ihtsdo.otf.tcc.api.store.TerminologyStoreDI;
import org.ihtsdo.otf.tcc.api.store.Ts;
import org.ihtsdo.otf.tcc.ddo.ResultList;
import org.ihtsdo.otf.tcc.ddo.concept.ConceptChronicleDdo;
import org.ihtsdo.otf.tcc.ddo.concept.component.description.DescriptionChronicleDdo;
import org.ihtsdo.otf.tcc.ddo.concept.component.description.SimpleDescriptionVersionDdo;
import org.ihtsdo.otf.tcc.ddo.fetchpolicy.RefexPolicy;
import org.ihtsdo.otf.tcc.ddo.fetchpolicy.RelationshipPolicy;
import org.ihtsdo.otf.tcc.ddo.fetchpolicy.VersionPolicy;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.ihtsdo.otf.tcc.model.index.service.IndexerBI;
import org.ihtsdo.otf.tcc.model.index.service.SearchResult;

/**
 *
 * @author dylangrald
 */
@Path("/descriptions")
@Api(value = "/descriptions", description = "Retrieves all active descriptions in SNOMED inferred latest from input SCTID.")
@Produces({"text/plain"})
public class DescriptionsForConceptResource {

    private static IndexerBI sctIdIndexer;
    private static final TerminologyStoreDI ts = Ts.get();

    static {
        List<IndexerBI> lookers = Hk2Looker.get().getAllServices(IndexerBI.class);

        for (IndexerBI li : lookers) {
            System.out.println("AlternativeIdResource found indexer: " + li.getIndexerName());
            if (li.getIndexerName().equals("refex")) {
                sctIdIndexer = li;
            }
        }
    }

    @GET
    @Produces("text/plain")
    public String getDescFromSctid() {
        return "Please enter SCTID.";
    }

    @GET
    @Path("/{id}")
    @Produces("text/plain")
    @ApiOperation(value = "Find descriptions from an input SCTID.", response = String.class)
    @ApiResponses(value = {
        @ApiResponse(code = 422, message = "No descriptions found for SCTID")
    })
    public String getDescFromSctid(
            @ApiParam(value = "Find all active descriptions for input SCTID.", required = true, defaultValue = "195967001")
            @PathParam("id") String id) throws IOException, JAXBException, Exception {
        System.out.println("Getting descriptions for: " + id);
        System.out.println("SCTID indexer: " + sctIdIndexer);

        if (!id.matches("[0-9]*") || id.length() > 18) {
            return "Incorrect SNOMED id.";
        }

        List<SearchResult> result = sctIdIndexer.query(id, ComponentProperty.LONG_EXTENSION_1, 1);
        System.out.println("result: " + result);
        for (SearchResult r : result) {
            System.out.println("nid: " + r.nid + " score:" + r.score);
        }
        System.out.println("result: " + result);

        if (!result.isEmpty()) {
            ViewCoordinate vc = StandardViewCoordinates.getSnomedInferredLatestActiveOnly();
            ComponentChronicleBI cc = Ts.get().getComponent(result.get(0).nid);
            UUID uuid = Ts.get().getUuidPrimordialForNid(cc.getNid());
            ConceptChronicleBI concept = Ts.get().getComponent(uuid).getEnclosingConcept();
            ConceptVersionBI cv = concept.getVersion(vc);

            ArrayList<Object> list = new ArrayList<>();

            for (DescriptionChronicleBI dc : concept.getVersion(vc).getDescriptions()) {
                if (dc.getVersion(vc) != null) {
                    DescriptionVersionBI dv = dc.getVersion(vc);
                    ConceptChronicleDdo ccDdo = new ConceptChronicleDdo(ts.getSnapshot(vc), concept, VersionPolicy.ACTIVE_VERSIONS, RefexPolicy.REFEX_MEMBERS, RelationshipPolicy.DESTINATION_RELATIONSHIPS);
                    DescriptionChronicleDdo dcDdo = new DescriptionChronicleDdo(ts.getSnapshot(vc), ccDdo, dc);
                    list.add(new SimpleDescriptionVersionDdo(dcDdo, ts.getSnapshot(vc), dv, cv));
                }
            }
            if (list.size() > 0) {

                ResultList resultList = new ResultList();
                resultList.setTheResults(list);
                StringWriter writer = new StringWriter();

                JaxbForQuery.get().createMarshaller().marshal(resultList, writer);
                return writer.toString();
            }
        }
        return "No descriptions found for " + id + ". Please ensure you have the correct SNOMED id.";

    }
}
