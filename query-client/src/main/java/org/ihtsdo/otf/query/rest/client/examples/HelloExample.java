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
package org.ihtsdo.otf.query.rest.client.examples;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This example validates a simple connection to the query service via a Apache
 * Jersey REST client. This simple example is intended to demonstrate proper
 * connectivity and service availability. This example takes advantage of a
 * hello service on the server that is just there for simple verification. When
 * a path of "query-service/hello/frank" is provided, it will return "hello
 * frank." Similarly, if "query-service/hello/bob" is provided, it will return
 * "hello bob."
 *
 * @author kec
 */
public class HelloExample {

    /**
     * 
     * @param args args[0] is an optional server url. 
     */
    public static void main(String[] args) {
        // default host.
        String host = "http://localhost:8080/otf/";
        
        // if host is provided, override default host.
        if (args.length > 0) {
            host = args[0];
        }
        // create the client
        Client client = ClientBuilder.newClient();
        // specify the host and the path. 
        WebTarget target = client.target(host).path("query-service/hello/frank");
        // get the response from the server
        Response response = target.request(MediaType.TEXT_PLAIN_TYPE).get();
        // should return "200"
        System.out.println(response.getStatus()); 
        // should return "hello frank."
        System.out.println(response.readEntity(String.class)); 
    }
}
