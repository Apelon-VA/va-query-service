/*
 * Copyright 2015 kec.
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
package gov.vha.isaac.cradle;

import gov.vha.isaac.cradle.component.ConceptChronicleDataEager;
import gov.vha.isaac.cradle.identifier.IdentifierProvider;
import gov.vha.isaac.ochre.api.IdentifierService;
import gov.vha.isaac.ochre.collections.NidSet;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ihtsdo.otf.tcc.model.cc.component.ConceptComponent;
import org.ihtsdo.otf.tcc.model.cc.refex.RefexMember;

/**
 *
 * @author kec
 */
public class IntegrityTest {
    private static final Logger log = LogManager.getLogger();

    public static void perform(CradleExtensions extensions, IdentifierService idService) {
        IdentifierProvider idProvider = (IdentifierProvider) idService;
        ConcurrentSequenceIntMap nidCnidMap = idProvider.getNidCnidMap();
        log.info("NidCnid integrity test. ");
        IntStream componentsNotSet = nidCnidMap.getComponentsNotSet();
        NidSet componentNidsNotSet
                = NidSet.of(componentsNotSet.map((nid) -> {
                    return nid - Integer.MIN_VALUE;
                }).toArray());
        componentNidsNotSet.remove(Integer.MIN_VALUE); // we know Integer.MIN_VALUE is not used. 
        log.info("Components with no concept: " + componentNidsNotSet.size());
        extensions.getParallelConceptDataEagerStream().forEach((ConceptChronicleDataEager cde) -> {
            if (componentNidsNotSet.contains(cde.getNid())) {
                if (nidCnidMap.containsKey(cde.getNid())) {
                    int key = nidCnidMap.get(cde.getNid()).getAsInt();
                    System.out.println("Concept in nidCnidMap, but not componentsNotSet: " + cde);
                    componentNidsNotSet.contains(cde.getNid());
                } else {
                    System.out.println("Concept not in nidCnidMap: " + cde);
                }
            }
            cde.getConceptComponents().forEach((ConceptComponent<?, ?> cc) -> {
                if (componentNidsNotSet.contains(cc.getNid())) {
                    if (nidCnidMap.containsKey(cde.getNid())) {
                        int key = nidCnidMap.get(cde.getNid()).getAsInt();
                        System.out.println("component in nidCnidMap, but not componentsNotSet: " + cc);
                        componentNidsNotSet.contains(cde.getNid());
                    } else {
                        System.out.println("component not in nidCnidMap: " + cc);
                    }
                }
            });
        });

        extensions.getParallelRefexStream().forEach((RefexMember<?, ?> sememe) -> {
            if (componentNidsNotSet.contains(sememe.getNid())) {
                if (nidCnidMap.containsKey(sememe.getNid())) {
                    int key = nidCnidMap.get(sememe.getNid()).getAsInt();
                    System.out.println("Sememe in nidCnidMap, but not componentsNotSet: " + sememe);
                    componentNidsNotSet.contains(sememe.getNid());
                } else {
                    System.out.println("Sememe not in nidCnidMap: " + sememe);
                }
            }
        });

        componentNidsNotSet.stream().limit(100).forEach((int nid) -> {
            List<UUID> uuids = idService.getUuidsForNid(nid);
            System.out.println("Unmapped nid: " + nid + " UUIDs:" + uuids);
        });
    }
    
}
