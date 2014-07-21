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
import javax.servlet.ServletRequest;
import javax.servlet.ServletRequestEvent;
import javax.servlet.ServletRequestListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author dylangrald
 */
public class QueryServletRequestListener implements ServletRequestListener {

    @Override
    public void requestDestroyed(ServletRequestEvent sre) {
        ServletRequest servletRequest = sre.getServletRequest();
        System.out.println("ServletRequest destroyed. Remote IP=" + servletRequest.getRemoteAddr());
    }

    @Override
    public void requestInitialized(ServletRequestEvent sre) {
        ServletRequest servletRequest = sre.getServletRequest();
        System.out.println("ServletRequest initialized. Remote IP=" + servletRequest.getRemoteAddr());
    }

    private void initializeQuery(ServletRequestEvent sre) throws QueryApplicationException {
        String status = sre.getServletContext().getAttribute("status").toString();
        if (status.equals("The project is building.")) {
            throw new QueryApplicationException(HttpErrorType.ERROR503, "Building the project. Please try again once the project has built and the database is open.");
        } else if (status.equals("Opening the database")) {
            throw new QueryApplicationException(HttpErrorType.ERROR503, "Opening the database. Please try again once the database is open.");
        }
    }

    private void notFound(HttpServletResponse response) {
        try {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
        } catch (IllegalStateException e) {
            ;
        } catch (IOException e) {
            ;
        }

    }
}
