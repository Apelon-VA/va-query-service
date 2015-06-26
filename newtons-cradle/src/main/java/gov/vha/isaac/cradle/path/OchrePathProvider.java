/*
 * Copyright 2015 U.S. Department of Veterans Affairs.
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
package gov.vha.isaac.cradle.path;

import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.IdentifierService;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.PathService;
import gov.vha.isaac.ochre.api.component.concept.ConceptService;
import gov.vha.isaac.ochre.api.component.sememe.SememeService;
import gov.vha.isaac.ochre.api.component.sememe.version.LongSememe;
import gov.vha.isaac.ochre.api.coordinate.StampPath;
import gov.vha.isaac.ochre.api.coordinate.StampPosition;
import gov.vha.isaac.ochre.model.coordinate.StampPathImpl;
import gov.vha.isaac.ochre.model.coordinate.StampPositionImpl;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * TODO handle versions properly in path provider.
 *
 * @author kec
 */
public class OchrePathProvider implements PathService {

    private static final Logger log = LogManager.getLogger();

    private static final Lock lock = new ReentrantLock();
    private static IdentifierService identifierService;

    private static IdentifierService getIdentifierService() {
        if (identifierService == null) {
            identifierService = LookupService.getService(IdentifierService.class);
        }
        return identifierService;
    }

    private static SememeService sememeService = null;

    private static SememeService getSememeService() {
        if (sememeService == null) {
            sememeService = LookupService.getService(SememeService.class);
        }
        return sememeService;
    }

    private static ConceptService conceptService = null;

    private static ConceptService getConceptService() {
        if (conceptService == null) {
            conceptService = LookupService.getService(ConceptService.class);
        }
        return conceptService;
    }
    //~--- fields --------------------------------------------------------------
    ConcurrentHashMap<Integer, StampPath> pathMap;

    //~--- constructors --------------------------------------------------------
    protected OchrePathProvider() {
        
    }

    //~--- methods -------------------------------------------------------------
    @Override
    public boolean exists(int pathConceptId) {
        setupPathMap();
        if (pathConceptId < 0) {
            pathConceptId = getIdentifierService().getConceptSequence(pathConceptId);
        }
        if (pathMap.containsKey(pathConceptId)) {
            return true;
        }
        Optional<StampPath> stampPath = getFromDisk(pathConceptId);
        return stampPath.isPresent();
    }

    private void setupPathMap() {
        if (pathMap == null) {
            lock.lock();
            try {
                pathMap = new ConcurrentHashMap<>();
                getSememeService().getSememesFromAssemblage(
                        IsaacMetadataAuxiliaryBinding.PATHS_ASSEMBLAGE.getSequence()).forEach((pathSememe) -> {
                            int pathSequence = getIdentifierService().getConceptSequence(pathSememe.getReferencedComponentNid());
                            pathMap.put(pathSequence,
                                    new StampPathImpl(pathSequence));
                        });
            } finally {
                lock.unlock();
            }
        }
    }

    private Optional<StampPath> getFromDisk(int stampPathSequence) {
        return getSememeService().getSememesForComponentFromAssemblage(stampPathSequence,
                IsaacMetadataAuxiliaryBinding.PATHS_ASSEMBLAGE.getSequence()).map((sememeChronicle) -> {
                    
                    int pathId = sememeChronicle.getReferencedComponentNid();
                    pathId = getIdentifierService().getConceptSequence(pathId);
                    assert pathId == stampPathSequence: "pathId: " + pathId + " stampPathSequence: " + stampPathSequence;
                    StampPath stampPath = new StampPathImpl(stampPathSequence);
                    pathMap.put(stampPathSequence, stampPath);
                    return stampPath;
                }).findFirst();
    }

    @Override
    public Collection<? extends StampPosition> getOrigins(int stampPathSequence) {
        setupPathMap();
        if (stampPathSequence < 0) {
            stampPathSequence = getIdentifierService().getConceptSequence(stampPathSequence);
        }
        return getPathOriginsFromDb(stampPathSequence);
    }

    private List<StampPosition> getPathOriginsFromDb(int nid) {
        return getSememeService().getSememesForComponentFromAssemblage(nid,
                IsaacMetadataAuxiliaryBinding.PATH_ORIGINS_ASSEMBLAGE.getSequence())
                .map((pathOrigin) -> {
                    long time = ((LongSememe) pathOrigin.getVersionList().get(0)).getLongValue();
                    return new StampPositionImpl(time, identifierService.getConceptSequence(nid));
                })
                .collect(Collectors.toList());
    }

    @Override
    public StampPath getStampPath(int stampPathSequence) {
        setupPathMap();
        if (stampPathSequence < 0) {
            stampPathSequence = getIdentifierService().getConceptSequence(stampPathSequence);
        }
        if (exists(stampPathSequence)) {
            return pathMap.get(stampPathSequence);
        }
        Optional<StampPath> stampPath = getFromDisk(stampPathSequence);
        if (stampPath.isPresent()) {
            return stampPath.get();
        }
        throw new IllegalStateException("No path for: " + stampPathSequence + 
                " " + getConceptService().getConcept(stampPathSequence).toString());
    }
}
