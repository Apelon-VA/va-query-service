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

import javax.xml.bind.annotation.XmlRootElement;

/**
 * The types of objects to be returned in the result set of a query.
 *
 * @author dylangrald
 */
@XmlRootElement
public enum ReturnTypes {

    /**
     * The native identifier of the component.
     */
    NIDS,
    /**
     * The UUID of the component.
     */
    UUIDS,
    /**
     * The component version of the results.
     */
    COMPONENT,
    /**
     * The Concept version specified by the
     * <code>ViewCoordinate</code> from the
     * <code>Query</code>.
     */
    CONCEPT_VERSION,
    /**
     * The Description version of the component. If the returned component is a
     * concept, then the description of the fully specified name will be
     * returned.
     */
    DESCRIPTION_FOR_COMPONENT,
    /**
     * Description version of the fully specified name.
     */
    DESCRIPTION_VERSION_FSN,
    /**
     * Description version of the preferred version.
     */
    DESCRIPTION_VERSION_PREFERRED;
}
