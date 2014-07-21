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

/**
 * Enumeration for semantics of query clauses.
 *
 * @author kec
 */
public enum ClauseSemantic {

    /**
     * Logical And connective clause.
     */
    AND,
    AND_NOT,
    /**
     * Logical OR connective clause.
     */
    OR,
    /**
     * Logical NOT unary operator.
     */
    NOT,
    /**
     * Logical XOR, used to determine what changed in membership between two
     * versions.
     */
    XOR,
    /**
     * Test to see if component has changed from the previous version.
     */
    CHANGED_FROM_PREVIOUS_VERSION,
    /**
     * Substitute the concept for any components matching criterion.
     */
    CONCEPT_FOR_COMPONENT,
    /**
     * Test to see if the concept is a logical child of another.
     */
    CONCEPT_IS_CHILD_OF,
    /**
     * Test to see if there is a concept matching the input concept spec.
     */
    CONCEPT_IS,
    /**
     * Test to see if the concept is a logical descendent of another.
     */
    CONCEPT_IS_DESCENDENT_OF,
    /**
     * Test to see if the concept is a logical kind of another.
     */
    CONCEPT_IS_KIND_OF,
    /**
     * Test to see if a component is a member of a refset.
     */
    COMPONENT_IS_MEMBER_OF_REFSET,
    /**
     * Test to see if an active description on a concept matches a Lucene query
     * criterion.
     */
    DESCRIPTION_ACTIVE_LUCENE_MATCH,
    /**
     * Test to see if an active description on a concept matches matches a regex
     * expression.
     */
    DESCRIPTION_ACTIVE_REGEX_MATCH,
    /**
     * Test to see if any description (active or inactive) on a concept matches
     * a Lucene query criterion.
     */
    DESCRIPTION_LUCENE_MATCH,
    /**
     * Test to see if any description (active or inactive) on a concept matches
     * matches a regex expression.
     */
    DESCRIPTION_REGEX_MATCH,
    /**
     * Substitute the fully specified description for a concept.
     */
    FULLY_SPECIFIED_NAME_FOR_CONCEPT,
    /**
     * Substitute the preferred name for a concept.
     */
    PREFERRED_NAME_FOR_CONCEPT,
    /**
     * Test to see if a refset string member matches a Lucene query criterion.
     */
    REFSET_LUCENE_MATCH,
    /**
     * Test to see if a relationship matches a specified relationship type.
     */
    REL_TYPE,
    /**
     * Test to see if a relationship matches a specified relationship type and
     * relationship destination restriction.
     */
    REL_RESTRICTION,
    /**
     * Test to see if a refset contains a specified concept.
     */
    REFSET_CONTAINS_CONCEPT,
    /**
     * Test to see if a refset contains a kind of specified concept.
     */
    REFSET_CONTAINS_KIND_OF_CONCEPT,
    /**
     * Test to see if a refset contains a member that matches the specified
     * string.
     */
    REFSET_CONTAINS_STRING,
}
