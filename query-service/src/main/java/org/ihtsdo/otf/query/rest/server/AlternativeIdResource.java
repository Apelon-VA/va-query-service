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

//~--- non-JDK imports --------------------------------------------------------
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.ihtsdo.otf.tcc.model.index.service.IndexerBI;

//~--- JDK imports ------------------------------------------------------------
import java.io.IOException;

import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import javax.xml.bind.JAXBException;
import org.ihtsdo.otf.query.lucene.LuceneRefexIndexer;
import org.ihtsdo.otf.tcc.api.blueprint.ComponentProperty;
import org.ihtsdo.otf.tcc.api.chronicle.ComponentChronicleBI;
import org.ihtsdo.otf.tcc.api.metadata.binding.TermAux;
import org.ihtsdo.otf.tcc.api.refex.RefexChronicleBI;
import org.ihtsdo.otf.tcc.api.refex.type_long.RefexLongVersionBI;
import org.ihtsdo.otf.tcc.api.spec.ValidationException;
import org.ihtsdo.otf.tcc.api.store.TerminologyStoreDI;
import org.ihtsdo.otf.tcc.api.store.Ts;
import org.ihtsdo.otf.tcc.model.index.service.SearchResult;

/**
 * This resource accepts SNOMED IDs, and returns the associated UUID for that
 * SNOMED ID, or it accepts UUIDs and returns the associated SNOMED ID for that
 * UUID.
 *
 * @author kec
 */
@Api(value = "/alternate-id", description = "Retrieve UUID from SCTID and vice-versa.")
@Path("/alternate-id")
@Produces({"text/plain"})
public class AlternativeIdResource {

    private static IndexerBI sctIdIndexer;
    private static volatile int snomedAssemblageNid = Integer.MIN_VALUE;
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
    @Path("/uuid/{id}")
    @Produces("text/plain")
    @ApiOperation(value = "Find UUID from SCTID", response = String.class)
    @ApiResponses(value = {
        @ApiResponse(code = 422, message = "No UUIDs found for SCTID")
    })
    public String getUuidFromSctid(
            @ApiParam(value = "Retrieve the UUID of a component from a SCTID. ", required = true, defaultValue = "195967001")
            @PathParam("id") String id) throws IOException, JAXBException, Exception {
        System.out.println("Getting UUID for: " + id);
        System.out.println("SCTID indexer: " + sctIdIndexer);

        List<SearchResult> result = sctIdIndexer.query(id, ComponentProperty.LONG_EXTENSION_1, 1);
        System.out.println("result: " + result);
        for (SearchResult r : result) {
            System.out.println("nid: " + r.nid + " score:" + r.score);
        }
        System.out.println("result: " + result);

        if (!result.isEmpty()) {
            ComponentChronicleBI cc = ts.getComponent(result.get(0).nid);
            RefexChronicleBI rx = (RefexChronicleBI) cc;
            UUID uuid = ts.getUuidPrimordialForNid(rx.getReferencedComponentNid());
            return uuid.toString();
        }
        return "No UUIDs found for " + id;
    }

    @GET
    @Path("/uuid")
    @Produces("text/plain")
    public String getUuidInfo() throws IOException, JAXBException, Exception {
        return "Add the SNOMED ID to the end of the URL";
    }

    @GET
    @Path("/sctid")
    @Produces("text/plain")
    public String getSctidInfo() throws IOException, JAXBException, Exception {
        return "Add the UUID to the end of the URL";
    }

    @GET
    @Path("/sctid/{id}")
    @Produces("text/plain")
    @ApiOperation(value = "Find SCTID from UUID", response = String.class)
    @ApiResponses(value = {
        @ApiResponse(code = 422, message = "No SCTIDs found for UUID")
    })
    public String getSctidFromUuid(
            @ApiParam(value = "Retrieve the SCTID corresponding to an input UUID. ", required = true, defaultValue = "c265cf22-2a11-3488-b71e-296ec0317f96")
            @PathParam("id") String id) throws IOException, JAXBException, Exception {
        System.out.println("Getting sctid for: " + id);
        if (snomedAssemblageNid == Integer.MIN_VALUE) {
            try {
                snomedAssemblageNid = TermAux.SNOMED_IDENTIFIER.getNid();
            } catch (ValidationException ex) {
                Logger.getLogger(LuceneRefexIndexer.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(LuceneRefexIndexer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        ComponentChronicleBI<?> component = ts.getComponent(UUID.fromString(id));
        if (component != null) {
            for (RefexChronicleBI<?> annotation : component.getAnnotations()) {
                if (annotation.getAssemblageNid() == snomedAssemblageNid) {
                    RefexLongVersionBI sctid = (RefexLongVersionBI) annotation.getPrimordialVersion();
                    return Long.toString(sctid.getLong1());
                }
            }
        }
        return "No SCTIDs found for " + id;
    }

}
