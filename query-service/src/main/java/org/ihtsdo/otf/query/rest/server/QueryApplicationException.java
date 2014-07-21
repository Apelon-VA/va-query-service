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

/**
 *
 * @author dylangrald
 */
import java.io.Serializable;
import javax.ws.rs.WebApplicationException;

public class QueryApplicationException extends WebApplicationException implements Serializable {

    private static final long serialVersionUID = 1L;
    StackTraceElement[] otherST;
    String stackTraceString;
    HttpErrorType errorType;
    String reason = "See full stack trace.\n\n";
    String otherExceptionReason = null;
    int status;

    public QueryApplicationException() {
        super();
    }

    public QueryApplicationException(HttpErrorType type) {
        super();
        this.errorType = type;
        this.status = type.getValue();
    }
    
    public QueryApplicationException(HttpErrorType type, String reason) {
        super();
        this.errorType = type;
        this.status = type.getValue();
        this.reason = reason;
    }

    public QueryApplicationException(String msg) {
        super();
    }

    public QueryApplicationException(HttpErrorType type, String reason, Exception ve) {
        this(type, reason);
        this.status = type.getValue();
        this.otherExceptionReason = ve.getMessage();
        this.otherST = ve.getStackTrace();
    }

    public String getStackTraceString(String exceptionName, StackTraceElement[] stackTrace) {
        StringBuilder buff = new StringBuilder();
        buff.append("\n").append(exceptionName).append(" details:\n\n");
        for (StackTraceElement s : stackTrace) {
            buff.append(s.toString()).append("\n");
        }
        this.stackTraceString = buff.toString();
        return stackTraceString;
    }

    public String getMoreInfoString() {
        return "\n\nSee the section on Query Client in the query documentation: \n"
                + "http://ihtsdo.github.io/OTF-Query-Services/query-documentation/docbook/query-documentation.html";
    }

    public String getReason() {
        return reason;
    }

    public String getOtherExceptionReason() {
        return otherExceptionReason;
    }

    public String getErrorTypeString() {
        switch (errorType) {
            case ERROR414:
                return "HTTP Status 414 - Request-URI Too Long. ";
            case ERROR422:
                return "HTTP Status 422 - Unprocessable Entity. ";
            case ERROR503:
                return "HTTP Status 503 - Service Unavailable. ";
            default:
                return "HTTP Status 500. ";
        }
    }

    @Override
    public String getMessage() {
        if (this.otherExceptionReason == null) {
            return getErrorTypeString() + getReason() + getStackTraceString("QueryApplicationException", this.getStackTrace()) + "\n\n" + getMoreInfoString();
        } else {
            return getErrorTypeString() + getReason() + getStackTraceString("QueryApplicationException", this.getStackTrace()) + "\n\n" + getOtherExceptionReason() + "\n\n" + getStackTraceString("ValidationException", this.otherST) + getReason() + "\n\n\n" + getMoreInfoString();
        }
    }
    
    public int getStatus(){
        return this.status;
    }
}