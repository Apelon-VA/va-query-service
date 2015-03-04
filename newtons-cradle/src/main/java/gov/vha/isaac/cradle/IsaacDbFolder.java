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

import static gov.vha.isaac.cradle.Cradle.DEFAULT_ISAACDB_FOLDER;
import gov.vha.isaac.lookup.constants.Constants;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author kec
 */
public class IsaacDbFolder {

    private static IsaacDbFolder singleton;
    private static final AtomicReference<Boolean> primordial = new AtomicReference<>();
    
    public static IsaacDbFolder get() throws IOException {
        if (singleton == null) {
            
            singleton = new IsaacDbFolder();
        }
        return singleton;
    }

    private Path dbFolderPath;

    
    public IsaacDbFolder() throws IOException {
        String issacDbRootFolder = System.getProperty(Constants.CHRONICLE_COLLECTIONS_ROOT_LOCATION_PROPERTY);
        if (issacDbRootFolder == null || issacDbRootFolder.isEmpty()) {
                throw new IllegalStateException(Constants.CHRONICLE_COLLECTIONS_ROOT_LOCATION_PROPERTY + 
                        " has not been set.");
        }
        

        dbFolderPath = Paths.get(issacDbRootFolder, DEFAULT_ISAACDB_FOLDER);
        primordial.compareAndSet(null, !Files.exists(dbFolderPath));
        if (primordial.get()) {
            Files.createDirectories(dbFolderPath);
        }        
    }

    public boolean getPrimordial() {
        return primordial.get();
    }

    public Path getDbFolderPath() {
        return dbFolderPath;
    }    
    
}
