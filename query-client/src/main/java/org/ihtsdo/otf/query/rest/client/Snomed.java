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

import org.ihtsdo.otf.jaxb.chronicle.api.SimpleConceptSpecification;

/**
 *
 * @author kec
 */
public class Snomed {
    
    private static SimpleConceptSpecification newSpec(String description, 
            String uuid) {
        SimpleConceptSpecification spec = new SimpleConceptSpecification();
        spec.setDescription(description);
        spec.setUuid(uuid);
        return spec;
    }

    public static SimpleConceptSpecification ABDOMINAL_WALL_STRUCTURE =
            newSpec("Abdominal wall structure (body structure)",
            "c258b290-fabc-3e9d-86f4-9cf4dc0d6978");
    public static SimpleConceptSpecification NEUROLOGICAL_SYMPTOM =
            newSpec("Neurological symptom (finding)",
                   "f40bbc47-9f5a-34d9-b3c8-3f58a08874fe");
    public static SimpleConceptSpecification BODY_STRUCTURE_REFSET =
            newSpec("Body structure (domain) refset",
                   "0acd09e5-a5f1-4880-82b2-2a5c273dca29");
    public static SimpleConceptSpecification BARANYS_SIGN =
            newSpec("Barany's sign (finding)",
                   "c8aa4892-6af9-3d70-acd9-bb249dc2ceaf");
    public static SimpleConceptSpecification RESPIRATORY_DISORDER =
            newSpec("Respiratory disorder",
                   "275f19cb-83fc-3f8c-992d-9ad866602c88");
    public static SimpleConceptSpecification BRONCHIAL_HYPERREACTIVITY =
            newSpec("BHR - Bronchial hyperreactivity",
                   "c265cf22-2a11-3488-b71e-296ec0317f96");
    public static SimpleConceptSpecification ALLERGIC_ASTHMA =
            newSpec("Allergic asthma",
                   "531abe20-8324-3db9-9104-8bcdbf251ac7");
    public static SimpleConceptSpecification BODY_STRUCTURE =
            newSpec("Body structures",
                   "4be3f62e-28d5-3bb4-a424-9aa7856a1790");
    public static SimpleConceptSpecification IS_A =
            newSpec("Is a (attribute)",
                   "c93a30b9-ba77-3adb-a9b8-4589c9f8fb25");
    public static SimpleConceptSpecification FINDING_SITE =
            newSpec("Finding site (attribute)",
                   "3a6d919d-6c25-3aae-9bc3-983ead83a928");
    public static SimpleConceptSpecification CLINICAL_FINDING =
            newSpec("Clinical finding (finding)",
                   "bd83b1dd-5a82-34fa-bb52-06f666420a1c");
    public static SimpleConceptSpecification FULLY_SPECIFIED_DESCRIPTION_TYPE =
            newSpec("fully specified name (description type)",
                   "5e1fe940-8faf-11db-b606-0800200c9a66");
    public static SimpleConceptSpecification CORE_NAMESPACE =
            newSpec("Core Namespace",
                   "d6bbe207-7b5c-3e32-a2a1-f9259a7260c1");
    public static SimpleConceptSpecification CORE_MODULE =
            newSpec("SNOMED CT core module",
                   "1b4f1ba5-b725-390f-8c3b-33ec7096bdca");
    public static SimpleConceptSpecification SNOMED_RELEASE_PATH =
            newSpec("SNOMED Core",
                   "8c230474-9f11-30ce-9cad-185a96fd03a2");
    public static SimpleConceptSpecification EXTENSION_0 =
            newSpec("Extension Namespace 1000000",
                   "18388bfd-9fab-3581-9e22-cbae53725ef2");
    public static SimpleConceptSpecification EXTENSION_13 =
            newSpec("Extension Namespace 1000013",
                   "bb57db0f-def7-3fb7-b7f2-89fa7710bffa");
    public static SimpleConceptSpecification CONCEPT_HISTORY_ATTRIB =
            newSpec("Concept history attribute",
                   "f323b5dd-1f97-3873-bcbc-3563663dda14");
    public static SimpleConceptSpecification PRODUCT =
            newSpec("Pharmaceutical / biologic product (product)",
                   "5032532f-6b58-31f9-84c1-4a365dde4449");
    public static SimpleConceptSpecification INACTIVE_CONCEPT =
            newSpec("Inactive concept (inactive concept)",
                   "f267fc6f-7c4d-3a79-9f17-88b82b42709a");
    //Concept Specs for context sensitive role relationships
    public static SimpleConceptSpecification ACCESS =
            newSpec("Access (attribute)",
                   "3f5a4b8c-923b-3df5-9362-67881b729394");
    public static SimpleConceptSpecification APPROACH =
            newSpec("Procedural approach (qualifier value)",
                   "2209583c-de0b-376d-9aa0-850c37240788");
    public static SimpleConceptSpecification ASSOCIATED_FINDING =
            newSpec("Associated finding (attribute)",
                   "b20b664d-2690-3092-a2ef-7f8013b2dad3");
    public static SimpleConceptSpecification ASSOCIATED_MORPHOLOGY =
            newSpec("Associated morphology (attribute)",
                   "3161e31b-7d00-33d9-8cbd-9c33dc153aae");
    public static SimpleConceptSpecification ASSOCIATED_WITH =
            newSpec("Associated with (attribute)",
                   "79e34041-f87c-3659-b033-41bdd35bd89e");
    public static SimpleConceptSpecification ASSOCIATED_WITH_AFTER =
            newSpec("After (attribute)",
                   "fb6758e0-442c-3393-bb2e-ff536711cde7");
    public static SimpleConceptSpecification ASSOCIATED_WITH_AGENT =
            newSpec("Causative agent (attribute)",
                   "f770e2d8-91e6-3c55-91be-f794ee835265");
    public static SimpleConceptSpecification ASSOCIATED_WITH_DUE =
            newSpec("Due to (attribute)",
                   "6525dbf8-c839-3e45-a4bb-8bab7faf7cf9");
    public static SimpleConceptSpecification CLINICAL_COURSE =
            newSpec("Clinical course (attribute)",
                   "0d8a9cbb-e21e-3de7-9aad-8223c000849f");
    public static SimpleConceptSpecification COMPONENT =
            newSpec("Component (attribute)",
                   "8f0696db-210d-37ab-8fe1-d4f949892ac4");
    public static SimpleConceptSpecification DIRECT_SUBSTANCE =
            newSpec("Direct substance (attribute)",
                   "49ee3912-abb7-325c-88ba-a98824b4c47d");
    public static SimpleConceptSpecification ENVIRONMENT =
            newSpec("Environment (environment)",
                   "da439d54-0823-3b47-abed-f9ba50791335");
    public static SimpleConceptSpecification EVENT =
            newSpec("Event (event)",
                   "c7243365-510d-3e5f-82b3-7286b27d7698");
    public static SimpleConceptSpecification FINDING_CONTEXT =
            newSpec("Finding context (attribute)",
                   "2dbbf50e-9e14-382d-80be-ec7a020cb436");
    public static SimpleConceptSpecification FINDING_INFORMER =
            newSpec("Finding informer (attribute)",
                   "4990c973-2c08-3972-93ed-3ce9cd4e1776");
    public static SimpleConceptSpecification FINDING_METHOD =
            newSpec("Finding method (attribute)",
                   "ee283805-ec23-3e22-8bd0-c739f8cbdd7d");
    public static SimpleConceptSpecification HAS_ACTIVE_INGREDIENT =
            newSpec("Has active ingredient (attribute)",
                   "65bf3b7f-c854-36b5-81c3-4915461020a8");
    public static SimpleConceptSpecification HAS_DEFINITIONAL_MANIFESTATION =
            newSpec("Has definitional manifestation (attribute)",
                   "545df979-75ea-3f82-939a-565d032bcdad");
    public static SimpleConceptSpecification HAS_DOSE_FORM =
            newSpec("Has dose form (attribute)",
                   "072e7737-e22e-36b5-89d2-4815f0529c63");
    public static SimpleConceptSpecification HAS_FOCUS =
            newSpec("Has focus (attribute)",
                   "b610d820-4486-3b5e-a2c1-9b66bc718c6d");
    public static SimpleConceptSpecification HAS_INTENT =
            newSpec("Has intent (attribute)",
                   "4e504dc1-c971-3e20-a4f9-b86d0c0490af");
    public static SimpleConceptSpecification HAS_INTERPRETATION =
            newSpec("Has interpretation (attribute)",
                   "993a598d-a95a-3235-813e-59252c975070");
    public static SimpleConceptSpecification HAS_SPECIMEN =
            newSpec("Has specimen (attribute)",
                   "5ce3e93b-8594-3d38-b410-b06039e63e3c");
    public static SimpleConceptSpecification INTERPRETS =
            newSpec("Interprets (attribute)",
                   "75e0da0c-21ea-301f-a176-bf056788afe5");
    public static SimpleConceptSpecification LATERALITY =
            newSpec("Laterality (attribute)",
                   "26ca4590-bbe5-327c-a40a-ba56dc86996b");
    public static SimpleConceptSpecification LINK_ASSERTION =
            newSpec("Link assertion (link assertion)",
                   "7f39edac-198d-366d-b8b9-4eab221ee144");
    public static SimpleConceptSpecification MEASUREMENT_METHOD =
            newSpec("Measurement method (attribute)",
                   "a6e4f659-a4b4-33b7-a75d-4a810167b32a");
    public static SimpleConceptSpecification METHOD =
            newSpec("Method (attribute)",
                   "d0f9e3b1-29e4-399f-b129-36693ba4acbc");
    public static SimpleConceptSpecification MORPHOLOGIC_ABNORMALITY =
            newSpec("Morphologically abnormal structure (morphologic abnormality)",
                   "3d3c4a6a-98d6-3a7c-9e1b-7fabf61e5ca5");
    public static SimpleConceptSpecification MOVED_ELSEWHERE =
            newSpec("Moved elsewhere (inactive concept)",
                   "e730d11f-e155-3482-a423-9637db3bc1a2");
    public static SimpleConceptSpecification OBSERVABLE_ENTITY =
            newSpec("Observable entity (observable entity)",
                   "d678e7a6-5562-3ff1-800e-ab070e329824");
    public static SimpleConceptSpecification OCCURRENCE =
            newSpec("Occurrence (attribute)",
                   "d99e2a70-243d-3bf2-967a-faee3265102b");
    public static SimpleConceptSpecification ORGANISM =
            newSpec("Organism (organism)",
                   "0bab48ac-3030-3568-93d8-aee0f63bf072");
    public static SimpleConceptSpecification PATHOLOGICAL_PROCESS =
            newSpec("Pathological process (attribute)",
                   "52542cae-017c-3fc4-bff0-97b7f620db28");
    public static SimpleConceptSpecification PERSON =
            newSpec("Person (person)",
                   "37c4cc1d-b35c-3080-80ac-b5e3a14c8a4b");
    public static SimpleConceptSpecification PHYSICAL_FORCE =
            newSpec("Physical force (physical force)",
                   "32213bf6-c073-3ce1-b0c7-9463e43af2f1");
    public static SimpleConceptSpecification PHYSICAL_OBJECT =
            newSpec("Physical object (physical object)",
                   "72765109-6b53-3814-9b05-34ebddd16592");
    public static SimpleConceptSpecification PRIORITY =
            newSpec("Priority (attribute)",
                   "77d496f0-d56d-3ab1-b3c4-b58969ddd078");
    public static SimpleConceptSpecification PROCEDURE =
            newSpec("Procedure (procedure)",
                   "bfbced4b-ad7d-30aa-ae5c-f848ccebd45b");
    public static SimpleConceptSpecification PROCEDURE_CONTEXT =
            newSpec("Procedure context (attribute)",
                   "6d2e9614-a93f-3835-b278-01650b17743a");
    public static SimpleConceptSpecification PROCEDURE_DEVICE =
            newSpec("Procedure device (attribute)",
                   "820447dc-ff12-3902-b752-6e5397d297ef");
    public static SimpleConceptSpecification PROCEDURE_DEVICE_DIRECT =
            newSpec("Direct device (attribute)",
                   "102422d3-6b68-3d16-a756-1df791d91e7f");
    public static SimpleConceptSpecification PROCEDURE_INDIRECT_DEVICE =
            newSpec("Indirect device (attribute)",
                   "9f4020b4-9949-3448-b43a-f3f5b0d44e2b");
    public static SimpleConceptSpecification PROCCEDURE_ACCESS_DEVICE =
            newSpec("Using access device (attribute)",
                   "857b607c-bed8-3432-b474-1a65e613f242");
    public static SimpleConceptSpecification PROCEDURE_USING_DEVICE =
            newSpec("Using device (attribute)",
                   "7ee6ba00-b099-3c34-bfc0-6c9366ad9eae");
    public static SimpleConceptSpecification PROCEDURE_MORPHOLOGY =
            newSpec("Procedure morphology (attribute)",
                   "c6456f56-c088-34f5-a85d-39a5fcf62411");
    public static SimpleConceptSpecification PROCEDURE_MORPHOLOGY_DIRECT =
            newSpec("Direct morphology (attribute)",
                   "f28dd2fb-7573-3c53-b42a-c8212c946738");
    public static SimpleConceptSpecification PROCEDURE_INDIRECT_MORPHOLOGY =
            newSpec("Indirect morphology (attribute)",
                   "f941f564-f7b1-3a4f-a2ed-f8e1787ee082");
    public static SimpleConceptSpecification PROCEDURE_SITE =
            newSpec("Procedure site (attribute)",
                   "78dd0334-4b9e-3c26-9266-356f8c5c43ed");
    public static SimpleConceptSpecification PROCEDURE_SITE_DIRECT =
            newSpec("Procedure site - Direct (attribute)",
                   "472df387-0193-300f-9184-85b59aa85416");
    public static SimpleConceptSpecification PROCEDURE_SITE_INDIRECT =
            newSpec("Procedure site - Indirect (attribute)",
                   "ac38de9e-2c97-37ed-a3e2-365a87ba1730");
    public static SimpleConceptSpecification PROPERTY =
            newSpec("Property (attribute)",
                   "066462e2-f926-35d5-884a-4e276dad4c2c");
    public static SimpleConceptSpecification QUALIFIER_VALUE =
            newSpec("Qualifier value (qualifier value)",
                   "ed6a9820-ba24-3917-b1b2-151e9c5a7a8d");
    public static SimpleConceptSpecification RECIPIENT_CATEGORY =
            newSpec("Recipient category (attribute)",
                   "e4233cb6-6b8f-3ae5-85e5-dab691a81ecd");
    public static SimpleConceptSpecification REVISION_STATUS =
            newSpec("Revision status (attribute)",
                   "7b15c5ab-ecf9-3dd4-932c-5e7ce2482ee4");
    public static SimpleConceptSpecification ROUTE_OF_ADMIN =
            newSpec("Route of administration (attribute)",
                   "ddbb95e5-aaf6-38f3-b400-dcfb1f85be91");
    public static SimpleConceptSpecification SCALE_TYPE =
            newSpec("Scale type (attribute)",
                   "087afdd2-23cd-34c3-93a4-09088dfd480c");
    public static SimpleConceptSpecification SITUATION_WITH_EXPLICIT_CONTEXT =
            newSpec("Situation with explicit context (situation)",
                   "27d03723-07c3-3de9-828b-76aa05a23438");
    public static SimpleConceptSpecification SOCIAL_CONTEXT =
            newSpec("Social context (social concept)",
                   "b89db478-21d5-3e51-972d-6c900f0ec436");
    public static SimpleConceptSpecification SPECIMEN =
            newSpec("Specimen (specimen)",
                   "3680e12d-c14c-39cb-ac89-2ae1fa125d41");
    public static SimpleConceptSpecification SPECIMEN_PROCEDURE =
            newSpec("Specimen procedure (attribute)",
                   "e81aa5e5-fcf6-3329-994d-3154576ac90d");
    public static SimpleConceptSpecification SPECIMEN_SOURCE_ID =
            newSpec("Specimen source identity (attribute)",
                   "4ae2b18c-db93-339c-8a9f-35e027007bf5");
    public static SimpleConceptSpecification SPECIMEN_SOURCE_MORPHOLOGY =
            newSpec("Specimen source morphology (attribute)",
                   "3dd1e927-005e-30ba-b3a4-0a67d538fefe");
    public static SimpleConceptSpecification SPECIMEN_SOURCE_TOPOGRAPHY =
            newSpec("Specimen source topography (attribute)",
                   "4aafafbc-b21f-30a6-b676-0224e6b001ab");
    public static SimpleConceptSpecification SPECIMEN_SUBSTANCE =
            newSpec("Specimen substance (attribute)",
                   "500b618d-2896-36ff-a020-c3c988f816f1");
    public static SimpleConceptSpecification SUBJECT_REL_CONTEXT =
            newSpec("Subject relationship context (attribute)",
                   "cbd2a57c-a28d-3494-9193-2189f2b618a2");
    public static SimpleConceptSpecification SUBSTANCE =
            newSpec("Substance (substance)",
                   "95f41098-8391-3f5e-9d61-4b019f1de99d");
    public static SimpleConceptSpecification TEMPORAL_CONTEXT =
            newSpec("Temporal context (attribute)",
                   "2c6acb71-a375-30b4-952d-63916ed74084");
    public static SimpleConceptSpecification TIME_ASPECT =
            newSpec("Time aspect (attribute)",
                   "350adfa7-8fd5-3b95-91f2-8119b500a464");
    public static SimpleConceptSpecification USING_ENERGY =
            newSpec("Using energy (attribute)",
                   "3050f9ea-e811-37f2-b132-ffa06afcfbbe");
    public static SimpleConceptSpecification USING_SUBSTANCE =
            newSpec("Using substance (attribute)",
                   "996261c3-3c12-3f09-8f14-e30e85e9e70d");
    public static SimpleConceptSpecification ACCELERATION =
            newSpec("Acceleration (physical force)",
                   "6ef49616-e2c7-3557-b7f1-456a2c5a5e54");
    public static SimpleConceptSpecification CENTRIFUGAL_FORCE =
            newSpec("Centrifugal force (physical force)",
                   "2b684fe1-8baf-34ef-9d2a-df03142c915a");
    public static SimpleConceptSpecification CONTINUED_MOVEMENT =
            newSpec("Continued movement (physical force)",
                   "5a6abaa5-adac-3c31-a32e-acf4710dca9d");
    public static SimpleConceptSpecification DECELERATION =
            newSpec("Deceleration (physical force)",
                   "02df5644-8644-3430-98de-f740ada3286b");
    public static SimpleConceptSpecification MOMENTUM =
            newSpec("Momentum (physical force)",
                   "20fc8b77-9137-385c-92cc-ed4cec5b33a2");
    public static SimpleConceptSpecification VIBRATION =
            newSpec("Vibration (physical force)",
                   "c3cfddd6-71bc-34f4-97bc-745f475c88ff");
    public static SimpleConceptSpecification MOTION =
            newSpec("Motion (physical force)",
                   "45a8fde8-535d-3d2a-b76b-95ab67718b41");
    public static SimpleConceptSpecification RECORD_OCCLUSAL =
            newSpec("Record occlusal registration (procedure)",
                   "d13e32a7-588d-323c-adb2-886e72be5c5e");
    public static SimpleConceptSpecification STATUS =
            newSpec("status (status type)",
                   "d944af55-86d9-33f4-bebd-a10bf3f4712c");
    public static SimpleConceptSpecification CLINICAL_FINDING_ABSENT =
            newSpec("Clinical finding absent (situation)",
                   "66387a1b-9bb6-361d-99f0-0a3147cad7f2");
    public static SimpleConceptSpecification DIABETES_MELLITUS =
            newSpec("Diabetes mellitus (disorder)",
                   "dd25374d-b4a9-3622-bba5-c82ae028631c");
}
