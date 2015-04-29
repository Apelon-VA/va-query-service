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

import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.State;
import gov.vha.isaac.ochre.api.chronicle.LatestVersion;
import gov.vha.isaac.ochre.api.commit.CommitService;
import gov.vha.isaac.ochre.api.coordinate.StampCoordinate;
import gov.vha.isaac.ochre.api.sememe.SememeService;
import gov.vha.isaac.ochre.api.sememe.SememeSnapshotService;
import gov.vha.isaac.ochre.api.sememe.version.SememeVersion;
import gov.vha.isaac.ochre.api.snapshot.calculator.RelativePositionCalculator;
import gov.vha.isaac.ochre.collections.SememeSequenceSet;
import gov.vha.isaac.ochre.collections.StampSequenceSet;
import gov.vha.isaac.ochre.model.sememe.SememeChronicleImpl;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 *
 * @author kec
 * @param <V>
 */
public class SememeSnapshotProvider<V extends SememeVersion> implements SememeSnapshotService<V> {

    private static CommitService commitService;

    private static CommitService getCommitService() {
        if (commitService == null) {
            commitService = LookupService.getService(CommitService.class);
        }
        return commitService;
    }
    Class<V> versionType;
    StampCoordinate stampCoordinate;
    SememeService sememeProvider;
    RelativePositionCalculator calculator;

    public SememeSnapshotProvider(Class<V> versionType, StampCoordinate stampCoordinate, SememeService sememeProvider) {
        this.versionType = versionType;
        this.stampCoordinate = stampCoordinate;
        this.sememeProvider = sememeProvider;
        this.calculator = RelativePositionCalculator.getCalculator(stampCoordinate);
    }

    @Override
    public Optional<LatestVersion<V>> getLatestSememeVersion(int sememeSequence) {
        SememeChronicleImpl sc = (SememeChronicleImpl) sememeProvider.getSememe(sememeSequence);
        IntStream stampSequences = sc.getVersionStampSequences();
        StampSequenceSet latestSequences = calculator.getLatestStampSequences(stampSequences);
        if (latestSequences.isEmpty()) {
            return Optional.empty();
        }
        LatestVersion<V> latest = new LatestVersion<>();
        latestSequences.stream().forEach((stampSequence) -> {
            latest.addLatest((V) sc.getVersionForStamp(stampSequence).get());
        });

        return Optional.of(latest);
    }

    @Override
    public Optional<LatestVersion<V>> getLatestSememeVersionIfActive(int sememeSequence) {
        SememeChronicleImpl sc = (SememeChronicleImpl) sememeProvider.getSememe(sememeSequence);
        IntStream stampSequences = sc.getVersionStampSequences();
        StampSequenceSet latestSequences = calculator.getLatestStampSequences(stampSequences);
        if (latestSequences.isEmpty()) {
            return Optional.empty();
        }
        if (latestSequences.stream().noneMatch((int stampSequence) -> getCommitService().getStatusForStamp(stampSequence) == State.ACTIVE)) {
            return Optional.empty();
        }
        LatestVersion<V> latest = new LatestVersion<>();
        latestSequences.stream().forEach((stampSequence) -> {
            if (commitService.getStatusForStamp(stampSequence) == State.ACTIVE) {
                latest.addLatest((V) sc.getVersionForStamp(stampSequence).get());
            }
        });
        if (latest.value() == null) {
            return Optional.empty();
        }
        return Optional.of(latest);
    }

    @Override
    public Stream<LatestVersion<V>> getLatestSememeVersionsFromAssemblage(int assemblageSequence) {
        return getLatestSememeVersions(sememeProvider.getSememeSequencesFromAssemblage(assemblageSequence));
    }
    
    private Stream<LatestVersion<V>> getLatestSememeVersions(SememeSequenceSet sememeSequenceSet) {
        return sememeSequenceSet.stream()
                .mapToObj((int sememeSequence) -> {
                    SememeChronicleImpl sc = (SememeChronicleImpl) sememeProvider.getSememe(sememeSequence);
                    IntStream stampSequences = sc.getVersionStampSequences();
                    StampSequenceSet latestStampSequences = calculator.getLatestStampSequences(stampSequences);
                    if (latestStampSequences.isEmpty()) {
                        return Optional.empty();
                    }
                    LatestVersion<V> latest = new LatestVersion<>();
                    latestStampSequences.stream().forEach((stampSequence) -> {
                        latest.addLatest((V) sc.getVersionForStamp(stampSequence).get());
                    });
                    return Optional.of(latest);
                }
                ).filter((optional) -> {
                    return optional.isPresent();
                }).map((optional) -> (LatestVersion<V>) optional.get());
        
    }

    @Override
    public Stream<LatestVersion<V>> getLatestActiveSememeVersionsFromAssemblage(int assemblageSequence) {
        return getLatestActiveSememeVersions(sememeProvider.getSememeSequencesFromAssemblage(assemblageSequence));
    }
    
    private Stream<LatestVersion<V>> getLatestActiveSememeVersions(SememeSequenceSet sememeSequenceSet) {
        return sememeSequenceSet.stream()
                .mapToObj((int sememeSequence) -> {
                    SememeChronicleImpl sc = (SememeChronicleImpl) sememeProvider.getSememe(sememeSequence);
                    IntStream stampSequences = sc.getVersionStampSequences();
                    StampSequenceSet latestStampSequences = calculator.getLatestStampSequences(stampSequences);
                    if (latestStampSequences.isEmpty()) {
                        return Optional.empty();
                    }
                    if (latestStampSequences.stream().noneMatch((int stampSequence) -> getCommitService().getStatusForStamp(stampSequence) == State.ACTIVE)) {
                        return Optional.empty();
                    }

                    LatestVersion<V> latest = new LatestVersion<>();
                    
                    // add active first, incase any contradictions are inactive. 
                    latestStampSequences.stream().filter((int stampSequence) -> 
                            getCommitService().getStatusForStamp(stampSequence) == State.ACTIVE).forEach((stampSequence) -> {
                        Optional<V> version = sc.getVersionForStamp(stampSequence);
                        if (version.isPresent()) {
                            latest.addLatest(version.get());
                        } else {
                            throw new NoSuchElementException("No version for stamp: " + 
                                    stampSequence + " in: " + sc);
                        }
                        
                    });                    
                    latestStampSequences.stream().filter((int stampSequence) -> 
                            getCommitService().getStatusForStamp(stampSequence) == State.INACTIVE).forEach((stampSequence) -> {
                        Optional<V> version = sc.getVersionForStamp(stampSequence);
                        if (version.isPresent()) {
                            latest.addLatest(version.get());
                        } else {
                            throw new NoSuchElementException("No version for stamp: " + 
                                    stampSequence + " in: " + sc);
                        }
                    }); 
                    
                    return Optional.of(latest);
                }
                ).filter((optional) -> {
                    return optional.isPresent();
                }).map((optional) -> (LatestVersion<V>) optional.get());        
    }

    @Override
    public Stream<LatestVersion<V>> getLatestSememeVersionsForComponent(int componentNid) {
        return getLatestSememeVersions(sememeProvider.getSememeSequencesForComponent(componentNid));
    }

    @Override
    public Stream<LatestVersion<V>> getLatestActiveSememeVersionsForComponent(int componentNid) {
        return getLatestActiveSememeVersions(sememeProvider.getSememeSequencesForComponent(componentNid));
    }

    @Override
    public Stream<LatestVersion<V>> getLatestSememeVersionsForComponentFromAssemblage(int componentNid, int assemblageSequence) {
        return getLatestSememeVersions(sememeProvider.getSememeSequencesForComponentFromAssemblage(componentNid, assemblageSequence));
    }

    @Override
    public Stream<LatestVersion<V>> getLatestActiveSememeVersionsForComponentFromAssemblage(int componentNid, int assemblageSequence) {
        return getLatestActiveSememeVersions(sememeProvider.getSememeSequencesForComponentFromAssemblage(componentNid, assemblageSequence));
    }

}
