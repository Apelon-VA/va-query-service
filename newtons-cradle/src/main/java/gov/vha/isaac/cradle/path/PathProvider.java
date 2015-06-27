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

import gov.vha.isaac.ochre.api.ConfigurationService;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.PathService;
import gov.vha.isaac.ochre.api.coordinate.StampPath;
import gov.vha.isaac.ochre.api.coordinate.StampPosition;
import java.io.IOException;
import java.util.Collection;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.glassfish.hk2.runlevel.RunLevel;
import org.ihtsdo.otf.tcc.model.path.PathManager;
import org.jvnet.hk2.annotations.Service;

/**
 *
 * @author kec
 */
@Service(name = "Path Provider")
@RunLevel(value = 2)


public class PathProvider implements PathService {
    private static final Logger log = LogManager.getLogger();

    PathService provider;

    public PathProvider() {
        switch (LookupService.getService(ConfigurationService.class).getConceptModel()) {
            case OCHRE_CONCEPT_MODEL:
                provider = new OchrePathProvider(); 
                break;

            case OTF_CONCEPT_MODEL:
                provider = new PathManager();
                break;

            default:
                throw new UnsupportedOperationException("Can't handle: "
                        + LookupService.getService(ConfigurationService.class).getConceptModel());
        }
    }
    @PostConstruct
    private void startMe() throws IOException {
        log.info("Starting PathProvider post-construct: " + LookupService.getService(ConfigurationService.class).getConceptModel());
    }
    @PreDestroy
    private void stopMe() throws IOException {
        log.info("Stopping PathProvider pre-destroy: " + LookupService.getService(ConfigurationService.class).getConceptModel());
    }
    
    @Override
    public StampPath getStampPath(int stampPathSequence) {
        return provider.getStampPath(stampPathSequence);
    }

    @Override
    public boolean exists(int pathConceptId) {
        return provider.exists(pathConceptId);
    }

    @Override
    public Collection<? extends StampPosition> getOrigins(int stampPathSequence) {
        return provider.getOrigins(stampPathSequence);
    }

}
