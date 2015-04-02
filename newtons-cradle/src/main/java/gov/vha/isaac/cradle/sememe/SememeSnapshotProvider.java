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
package gov.vha.isaac.cradle.sememe;

import gov.vha.isaac.ochre.api.chronicle.LatestVersion;
import gov.vha.isaac.ochre.api.coordinate.StampCoordinate;
import gov.vha.isaac.ochre.api.sememe.SememeService;
import gov.vha.isaac.ochre.api.sememe.SememeSnapshotService;
import gov.vha.isaac.ochre.api.sememe.version.SememeVersion;
import gov.vha.isaac.ochre.model.sememe.SememeChronicleImpl;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 *
 * @author kec
 * @param <V>
 */
public class SememeSnapshotProvider<V extends SememeVersion> implements SememeSnapshotService<V> {

    Class<V> versionType;
    StampCoordinate stampCoordinate;
    SememeService sememeProvider;

    public SememeSnapshotProvider(Class<V> versionType, StampCoordinate stampCoordinate, SememeService sememeProvider) {
        this.versionType = versionType;
        this.stampCoordinate = stampCoordinate;
        this.sememeProvider = sememeProvider;
    }
    
    @Override
    public Optional<LatestVersion<V>> getLatestSememeVersion(int sememeSequence) {
        SememeChronicleImpl sc = (SememeChronicleImpl) sememeProvider.getSememe(sememeSequence);
        IntStream stampSequences = sc.getVersionStampSequences();
        
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Optional<LatestVersion<V>> getLatestSememeVersionIfActive(int sememeSequence) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Stream<LatestVersion<V>> getLatestSememeVersionsFromAssemblage(int assemblageSequence) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Stream<LatestVersion<V>> getLatestActiveSememeVersionsFromAssemblage(int assemblageSequence) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Stream<LatestVersion<V>> getLatestSememeVersionsForComponent(int componentNid) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Stream<LatestVersion<V>> getLatestActiveSememeVersionsForComponent(int componentNid) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Stream<LatestVersion<V>> getLatestSememeVersionsForComponentFromAssemblage(int componentNid, int assemblageSequence) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Stream<LatestVersion<V>> getLatestActiveSememeVersionsForComponentFromAssemblage(int componentNid, int assemblageSequence) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
