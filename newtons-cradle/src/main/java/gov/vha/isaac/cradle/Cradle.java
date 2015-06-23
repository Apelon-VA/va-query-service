package gov.vha.isaac.cradle;

import gov.vha.isaac.cradle.concept.ConceptProviderOtfModel;
import gov.vha.isaac.cradle.tasks.*;
import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordPrimitive;
import gov.vha.isaac.cradle.waitfree.CasSequenceObjectMap;
import gov.vha.isaac.cradle.component.ConceptChronicleDataEager;
import gov.vha.isaac.ochre.api.memory.MemoryUtil;
import org.ihtsdo.otf.tcc.model.cc.refex.RefexService;
import gov.vha.isaac.cradle.taxonomy.DestinationOriginRecord;
import gov.vha.isaac.cradle.taxonomy.CradleTaxonomyProvider;
import gov.vha.isaac.metadata.coordinates.ViewCoordinates;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.ConceptModel;
import gov.vha.isaac.ochre.api.ConceptProxy;
import gov.vha.isaac.ochre.api.IdentifiedObjectService;
import gov.vha.isaac.ochre.api.LookupService;
import javafx.concurrent.Task;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.glassfish.hk2.runlevel.RunLevel;
import org.ihtsdo.otf.tcc.api.blueprint.TerminologyBuilderBI;
import org.ihtsdo.otf.tcc.api.conattr.ConceptAttributeVersionBI;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.api.concept.ConceptVersionBI;
import org.ihtsdo.otf.tcc.api.concept.ProcessUnfetchedConceptDataBI;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.api.coordinate.EditCoordinate;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.api.coordinate.ViewCoordinate;
import org.ihtsdo.otf.tcc.api.cs.ChangeSetPolicy;
import org.ihtsdo.otf.tcc.api.cs.ChangeSetWriterThreading;
import org.ihtsdo.otf.tcc.api.db.DbDependency;
import org.ihtsdo.otf.tcc.api.description.DescriptionVersionBI;
import org.ihtsdo.otf.tcc.api.nid.NativeIdSetBI;
import org.ihtsdo.otf.tcc.api.nid.NidSetBI;
import org.ihtsdo.otf.tcc.api.refex.RefexChronicleBI;
import org.ihtsdo.otf.tcc.api.refexDynamic.RefexDynamicChronicleBI;
import org.ihtsdo.otf.tcc.api.relationship.RelationshipVersionBI;
import org.ihtsdo.otf.tcc.ddo.ComponentReference;
import org.ihtsdo.otf.tcc.ddo.concept.ConceptChronicleDdo;
import org.ihtsdo.otf.tcc.ddo.fetchpolicy.RefexPolicy;
import org.ihtsdo.otf.tcc.ddo.fetchpolicy.RelationshipPolicy;
import org.ihtsdo.otf.tcc.model.cc.NidPairForRefex;
import org.ihtsdo.otf.tcc.model.cc.concept.ConceptChronicle;
import org.ihtsdo.otf.tcc.model.cc.refex.RefexMember;
import org.ihtsdo.otf.tcc.model.cc.relationship.Relationship;
import org.ihtsdo.otf.tcc.model.cc.termstore.TerminologySnapshot;
import org.ihtsdo.otf.tcc.model.cc.termstore.Termstore;
import org.jvnet.hk2.annotations.Service;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import gov.vha.isaac.ochre.api.ConfigurationService;
import gov.vha.isaac.ochre.api.ObjectChronicleTaskService;
import gov.vha.isaac.ochre.api.IdentifierService;
import gov.vha.isaac.ochre.api.StandardPaths;
import gov.vha.isaac.ochre.api.SystemStatusService;
import gov.vha.isaac.ochre.api.TaxonomyService;
import gov.vha.isaac.ochre.api.chronicle.ObjectChronology;
import gov.vha.isaac.ochre.api.chronicle.StampedVersion;
import gov.vha.isaac.ochre.api.commit.CommitService;
import gov.vha.isaac.ochre.api.component.concept.ConceptChronology;
import gov.vha.isaac.ochre.api.component.concept.ConceptService;
import gov.vha.isaac.ochre.api.component.sememe.SememeService;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import gov.vha.isaac.ochre.collections.NidSet;
import gov.vha.isaac.ochre.util.WorkExecutors;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.ihtsdo.otf.tcc.api.concept.ConceptFetcherBI;
import org.ihtsdo.otf.tcc.api.nid.ConcurrentBitSet;
import org.ihtsdo.otf.tcc.api.nid.IntSet;
import org.ihtsdo.otf.tcc.dto.TtkConceptChronicle;

/**
 * Created by kec on 12/18/14.
 */
@Service(name = "ChRonicled Assertion Database of Logical Expressions")
@RunLevel(value = 2)
public class Cradle
        extends Termstore
        implements ObjectChronicleTaskService, CradleExtensions, IdentifiedObjectService {

    public static final String DEFAULT_CRADLE_FOLDER = "cradle";
    private static final Logger log = LogManager.getLogger();

    transient HashMap<UUID, ViewCoordinate> viewCoordinates = new HashMap<>();

    private static final AtomicReference<Boolean> cradleStartedEmpty = new AtomicReference<>();
    private static final AtomicReference<Path> cradlePath_ = new AtomicReference<>();

    ConceptService conceptProvider;
    IdentifierService identifierProvider;
    TaxonomyService taxonomyProvider;
    CommitService commitService;
    SememeService sememeProvider;
    RefexService refexProvider;
    ConceptModel conceptModel;

    public static Path getCradlePath() {
        cradlePath_.compareAndSet(null, new Supplier<Path>() {
            @Override
            public Path get() {
                ConfigurationService configurationService = LookupService.getService(ConfigurationService.class);
                Path cradlePath = configurationService.getChronicleFolderPath().resolve(DEFAULT_CRADLE_FOLDER);

                if (cradleStartedEmpty.compareAndSet(null, !Files.exists(cradlePath))) {
                    if (cradleStartedEmpty.get()) {
                        try {
                            Files.createDirectories(cradlePath);
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                }
                return cradlePath;
            }
        }.get());
        return cradlePath_.get();
    }

    public static boolean cradleStartedEmpty() {
        if (cradleStartedEmpty.get() == null) {
            //populated as a side effect
            getCradlePath();
        }
        return cradleStartedEmpty.get();
    }

    //For HK2
    private Cradle() throws IOException, NumberFormatException, ParseException {
        try {
            log.info("Setting up cradle at " + getCradlePath().toAbsolutePath().toString());
        } catch (Exception e) {
            LookupService.getService(SystemStatusService.class).notifyServiceConfigurationFailure("ChRonicled Assertion Database of Logical Expressions", e);
            throw e;
        }
    }

    @PostConstruct
    private void startMe() throws IOException {
        try {
            log.info("Starting Cradle post-construct");
            conceptProvider = LookupService.getService(ConceptService.class);
            commitService = LookupService.getService(CommitService.class);
            identifierProvider = LookupService.getService(IdentifierService.class);
            sememeProvider = LookupService.getService(SememeService.class);
            refexProvider = LookupService.getService(RefexService.class);
            taxonomyProvider = LookupService.getService(TaxonomyService.class);
            conceptModel = LookupService.getService(ConfigurationService.class).getConceptModel();
            MemoryUtil.startListener();
        } catch (Exception e) {
            LookupService.getService(SystemStatusService.class).notifyServiceConfigurationFailure("ChRonicled Assertion Database of Logical Expressions", e);
            throw e;
        }

    }

    @PreDestroy
    private void stopMe() throws IOException {
        log.info("Stopping Cradle pre-destroy. ");
        //integrityTest();

        cradleStartedEmpty.set(null);  //In case someone wants stop, then restart the service, we need to repopulate the vars about reading the DB.
        log.info("Finished pre-destroy.");
    }

    public void integrityTest() {
        log.info("NidCnid integrity test. ");
        IntegrityTest.perform(this, identifierProvider);
    }

    @Override
    public void writeRefex(RefexMember<?, ?> refex) {
        refexProvider.writeRefex(refex);
    }

    @Override
    public Stream<RefexMember<?, ?>> getRefexStream() {
        return refexProvider.getRefexStream();
    }

    @Override
    public Stream<RefexMember<?, ?>> getParallelRefexStream() {
        return refexProvider.getParallelRefexStream();
    }

    @Override
    public Stream<ConceptChronicleDataEager> getConceptDataEagerStream() {
        return ((ConceptProviderOtfModel) conceptProvider).getConceptDataEagerStream();
    }

    @Override
    public Stream<ConceptChronicleDataEager> getConceptDataEagerStream(ConceptSequenceSet conceptSequences) {
        return ((ConceptProviderOtfModel) conceptProvider).getConceptDataEagerStream(conceptSequences);
    }

    @Override
    public Stream<ConceptChronicleDataEager> getParallelConceptDataEagerStream(ConceptSequenceSet conceptSequences) {
        return ((ConceptProviderOtfModel) conceptProvider).getParallelConceptDataEagerStream(conceptSequences);
    }

    @Override
    public Stream<ConceptChronicleDataEager> getParallelConceptDataEagerStream() {
        return ((ConceptProviderOtfModel) conceptProvider).getParallelConceptDataEagerStream();
    }

    @Override
    public CharSequence informAboutObject(int nid) {
        if (nid > -1) {
            nid = identifierProvider.getConceptNid(nid);
        }
        StringBuilder sb = new StringBuilder();

        int conceptSequence = identifierProvider.getConceptSequenceForComponentNid(nid);
        if (conceptSequence == nid) {
            ConceptChronology<? extends StampedVersion> cc = conceptProvider.getConcept(conceptSequence);

            sb.append("'");
            sb.append(cc.toUserString());
            sb.append("' ");
            sb.append(conceptSequence);
            sb.append(" ");
            sb.append(cc.getPrimordialUuid());
        } else {
            Optional<? extends ObjectChronology<? extends StampedVersion>> component = getIdentifiedObjectChronology(nid);

            sb.append("comp: '");

            if (component.isPresent()) {
                sb.append(component.get().toUserString());
            } else {
                sb.append("null");
            }

            sb.append("' ");
            sb.append(nid);
            sb.append(" ");
            if (component.isPresent()) {
                sb.append(component.get().getPrimordialUuid());
            }
        }

        return sb;
    }

    @Override
    public CharSequence informAboutNid(int nid) {
        return informAboutObject(nid);

    }

    @Override
    public Stream<ConceptChronicle> getConceptStream() {
        return ((ConceptProviderOtfModel) conceptProvider).getConceptStream();
    }

    @Override
    public Stream<ConceptChronicle> getParallelConceptStream() {
        return ((ConceptProviderOtfModel) conceptProvider).getParallelConceptStream();
    }

    @Override
    public Stream<? extends ConceptChronicleBI> getConceptStream(ConceptSequenceSet conceptSequences) throws IOException {
        return ((ConceptProviderOtfModel) conceptProvider).getConceptStream(conceptSequences);
    }

    @Override
    public Stream<? extends ConceptChronicleBI> getParallelConceptStream(ConceptSequenceSet conceptSequences) throws IOException {
        return ((ConceptProviderOtfModel) conceptProvider).getConceptStream(conceptSequences).parallel();
    }

    @Override
    public int getNidForUuids(UUID... uuids) {
        return identifierProvider.getNidForUuids(uuids);

    }

    @Override
    public int loadEconFiles(File... files) throws Exception {
        java.nio.file.Path[] paths = new java.nio.file.Path[files.length];
        for (int i = 0; i < files.length; i++) {
            paths[i] = files[i].toPath();
        }
        return loadEconFiles(paths);
    }

    @Override
    public Task<Integer> startImportLogTask(Path... paths) {
        ImportCradleLogFile importCradleLogFile = new ImportCradleLogFile(paths, this);
        LookupService.getService(WorkExecutors.class).getForkJoinPoolExecutor().execute(importCradleLogFile);
        return importCradleLogFile;
    }

    @Override
    public Task<Integer> startLoadTask(java.nio.file.Path... paths) {
        ImportEConceptFile loaderTask = ImportEConceptFile.create(paths, this);
        return loaderTask;
    }

    @Override
    public Task<Integer> startExportTask(Path path) {
        return ExportEConceptFile.create(path, this);
    }

    @Override
    public Task<Integer> startLogicGraphExportTask(Path path) {
        return ExportEConceptFile.create(path, this,
                (Consumer<TtkConceptChronicle>) (TtkConceptChronicle t) -> {
                    t.setRelationships(null);
                });
    }

    @Override
    public int loadEconFiles(java.nio.file.Path... paths) throws Exception {
        return startLoadTask(paths).get();
    }

    @Override
    public ConceptChronicleDataEager getConceptData(int i) throws IOException {
        return ((ConceptProviderOtfModel) conceptProvider).getConceptData(i);
    }

    @Override
    public void addUncommitted(ConceptChronicleBI conceptChronicleBI) throws IOException {
        commitService.addUncommitted(conceptChronicleBI);
    }

    /**
     *
     * @param conceptChronicleBI
     * @throws IOException
     */
    @Override
    public void addUncommittedNoChecks(ConceptChronicleBI conceptChronicleBI) throws IOException {
        commitService.addUncommittedNoChecks(conceptChronicleBI);
    }

    // TODO getUncommittedConcepts() is unsupported.  Should be implemented or removed.
    @Override
    public Collection<? extends ConceptChronicleBI> getUncommittedConcepts() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean commit(ConceptChronicleBI conceptChronicleBI,
            ChangeSetPolicy changeSetPolicy,
            ChangeSetWriterThreading changeSetWriterThreading) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancel(ConceptChronicleBI conceptChronicleBI) throws IOException {
        commitService.cancel(conceptChronicleBI);
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit(ConceptChronicleBI cc) throws IOException {
        commitService.commit(cc, null);
    }

    @Override
    public void commit(ConceptVersionBI conceptVersionBI) throws IOException {
        commitService.commit(conceptVersionBI, null);
    }

    @Override
    public void forget(ConceptChronicleBI conceptChronicleBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NativeIdSetBI isChildOfSet(int parent, ViewCoordinate viewCoordinate) {
        IntStream childrenSequences = taxonomyProvider.getTaxonomyChildSequences(identifierProvider.getConceptSequence(parent), viewCoordinate);
        NativeIdSetBI childNidSet = new IntSet();
        childrenSequences.forEach((sequence) -> childNidSet.add(identifierProvider.getConceptNid(sequence)));
        return childNidSet;
    }

    @Override
    public NativeIdSetBI relationshipSet(int i, ViewCoordinate viewCoordinate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<DbDependency> getLatestChangeSetDependencies() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int[] getPossibleChildren(int i, ViewCoordinate viewCoordinate) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public TerminologyBuilderBI getTerminologyBuilder(EditCoordinate editCoordinate, ViewCoordinate viewCoordinate) {
        return new Builder(editCoordinate, viewCoordinate, this);
    }

    @Override
    public UUID getUuidPrimordialForNid(int nid) throws IOException {
        Optional<UUID> optionalUuid = identifierProvider.getUuidPrimordialForNid(nid);
        if (optionalUuid.isPresent()) {
            return optionalUuid.get();
        }
        return null;
    }

    @Override
    public List<UUID> getUuidsForNid(int nid) throws IOException {
        return identifierProvider.getUuidsForNid(nid);
    }

    @Override
    public boolean hasPath(int i) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasUncommittedChanges() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasUuid(UUID uuid) {
        return identifierProvider.hasUuid(uuid);
    }

    @Override
    public boolean hasUuid(List<UUID> uuids) {
        return identifierProvider.hasUuid(uuids);
    }

    @Override
    public long getSequence() {
        return commitService.getCommitManagerSequence();
    }

    @Override
    public int getConceptCount() throws IOException {
        return conceptProvider.getConceptCount();
    }

    @Override
    public ViewCoordinate getViewCoordinate(UUID uuid) throws IOException {
        return viewCoordinates.get(uuid);
    }

    @Override
    public Collection<ViewCoordinate> getViewCoordinates() throws IOException {
        return viewCoordinates.values();
    }

    @Override
    public void putViewCoordinate(ViewCoordinate viewCoordinate) throws IOException {
        viewCoordinates.put(viewCoordinate.getVcUuid(), viewCoordinate);
    }

    @Override
    public boolean isKindOf(int childNid, int parentNid, ViewCoordinate viewCoordinate) throws IOException, ContradictionException {
        return taxonomyProvider.isKindOf(childNid, parentNid, viewCoordinate);
    }

    @Override
    public boolean isChildOf(int childNid, int parentNid, ViewCoordinate viewCoordinate) throws IOException, ContradictionException {
        return taxonomyProvider.isChildOf(childNid, parentNid, viewCoordinate);
    }

    @Override
    public NativeIdSetBI isKindOfSet(int kindOfId, ViewCoordinate viewCoordinate) {
        try {
            ConcurrentBitSet bitSet = new ConcurrentBitSet(getConceptCount());
            ConceptSequenceSet sequenceSet
                    = taxonomyProvider.getKindOfSequenceSet(identifierProvider.getConceptSequence(kindOfId), viewCoordinate);
            sequenceSet.stream().forEach((int sequence) -> {
                bitSet.set(identifierProvider.getConceptNid(sequence));
            });
            return bitSet;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void shutdown() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxReadOnlyStamp() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasConcept(UUID cUUID) {
        //first call hasUuid, because this checks if it exists without storing it.
        if (!hasUuid(cUUID)) {
            return false;
        }
        //If we do have a UUID, check if we have a concept.  Don't want to call this first, as it permanently stores the UUID as a side effect.
        return hasConcept(getNidForUuids(cUUID));
    }

    @Override
    public boolean hasConcept(int i) {
        return identifierProvider.getUuidPrimordialForNid(i).isPresent();
    }

    @Override
    public long getLastCancel() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLastCommit() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long incrementAndGetSequence() {
        return commitService.incrementAndGetSequence();
    }

    @Override
    public void waitTillWritesFinished() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String> getProperties() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getProperty(String s) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setProperty(String s, String s2) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancelAfterCommit(NidSetBI nidSetBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addXrefPair(int i, NidPairForRefex nidPairForRefex) throws IOException {
        // noting to do, added elsewhere...
    }

    @Override
    protected ViewCoordinate makeMetaVc() throws IOException {
        return ViewCoordinates.getMetadataViewCoordinate();
    }

    @Override
    public void forgetXrefPair(int referencedComponentNid, NidPairForRefex nidPairForRefex) throws IOException {
        refexProvider.forgetXrefPair(referencedComponentNid, nidPairForRefex);
    }

    @Override
    public List<NidPairForRefex> getRefexPairs(int i) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int[] getDestRelOriginNids(int i, NidSetBI nidSetBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int[] getDestRelOriginNids(int i) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     *
     * @param i
     * @return
     * @throws IOException
     * @deprecated relationships are being replaced by owl 2 DL definitions, so
     * using rels directly is discouraged.
     */
    @Override
    @Deprecated
    public Collection<Relationship> getDestRels(int i) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setConceptNidForNid(int cNid, int nid) {
        identifierProvider.setConceptSequenceForComponentNid(cNid, nid);
    }

    @Override
    public void resetConceptNidForNid(int cNid, int nid) {
        identifierProvider.resetConceptSequenceForComponentNid(cNid, nid);
    }

    @Override
    public void addRelOrigin(int i, int i2) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(UUID uuid, int i) {
        identifierProvider.addUuidForNid(uuid, i);
    }

    /**
     * TODO remove isIndexed/setIndexed, replaced by Lucene Indexing.
     *
     * @param i
     * @param b
     */
    @Override
    @Deprecated
    public void setIndexed(int i, boolean b) {

    }

    /**
     * TODO remove isIndexed/setIndexed, replaced by Lucene Indexing.
     *
     * @param i
     * @return
     */
    @Override
    @Deprecated
    public boolean isIndexed(int i) {
        return false;
    }

    @Override
    public ConceptChronicleDdo getFxConcept(UUID uuid, ViewCoordinate viewCoordinate) throws IOException, ContradictionException {
        ConceptChronicleBI chronicle = this.getConcept(uuid);
        ConceptChronicleDdo c = new ConceptChronicleDdo(viewCoordinate, chronicle, RefexPolicy.REFEX_MEMBERS,
                RelationshipPolicy.ORIGINATING_RELATIONSHIPS);
        return c;
    }

    @Override
    public ConceptChronicleDdo getFxConcept(ComponentReference componentReference, UUID viewCoordinateUuid,
            RefexPolicy refexPolicy, RelationshipPolicy relationshipPolicy) throws IOException, ContradictionException {
        int nid = componentReference.getNid();
        ConceptChronicleBI chronicle = this.getConcept(nid);
        return new ConceptChronicleDdo(this.getViewCoordinate(viewCoordinateUuid), chronicle, refexPolicy, RelationshipPolicy.ORIGINATING_RELATIONSHIPS);
    }

    @Override
    public ConceptChronicleDdo getFxConcept(ComponentReference componentReference, ViewCoordinate viewCoordinate,
            RefexPolicy refexPolicy, RelationshipPolicy relationshipPolicy) throws IOException, ContradictionException {
        int nid = componentReference.getNid();
        ConceptChronicleBI chronicle = this.getConcept(nid);
        return new ConceptChronicleDdo(viewCoordinate, chronicle, refexPolicy, relationshipPolicy);
    }

    @Override
    public ConceptChronicleDdo getFxConcept(UUID uuid, UUID viewCoordinateUuid, RefexPolicy refexPolicy,
            RelationshipPolicy relationshipPolicy) throws IOException, ContradictionException {
        ConceptChronicleBI chronicle = this.getConcept(uuid);
        return new ConceptChronicleDdo(this.getViewCoordinate(viewCoordinateUuid), chronicle, refexPolicy, relationshipPolicy);
    }

    @Override
    public ConceptChronicleDdo getFxConcept(UUID uuid, ViewCoordinate viewCoordinate,
            RefexPolicy refexPolicy, RelationshipPolicy relationshipPolicy) throws IOException, ContradictionException {
        ConceptChronicleBI chronicle = this.getConcept(uuid);
        return new ConceptChronicleDdo(viewCoordinate, chronicle, refexPolicy, relationshipPolicy);
    }

    @Override
    public void cancel() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancel(ConceptVersionBI conceptVersionBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit() throws IOException {
        commitService.commit(null);
    }

    @Override
    public boolean forget(ConceptAttributeVersionBI conceptAttributeVersionBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forget(DescriptionVersionBI descriptionVersionBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forget(RefexChronicleBI refexChronicleBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forget(RefexDynamicChronicleBI refexDynamicChronicleBI) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forget(RelationshipVersionBI relationshipVersionBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<? extends ObjectChronology<? extends StampedVersion>> getIdentifiedObjectChronology(int nid) {
        switch (identifierProvider.getChronologyTypeForNid(nid)) {
            case CONCEPT:
                return Optional.ofNullable(conceptProvider.getConcept(identifierProvider.getConceptSequence(nid)));
            case SEMEME:
                return Optional.ofNullable(sememeProvider.getSememe(identifierProvider.getSememeSequence(nid)));
            case REFEX:
                return Optional.ofNullable(refexProvider.getRefex(identifierProvider.getRefexSequence(nid)));
        }
        //The above code doesn't identify descriptions - try to find them...
        //if (conceptModel == ConceptModel.OCHRE_CONCEPT_MODEL) {  //TODO Keith - not sure if this was a mistake... (you intended OTF_CONCEPT_MODEL)
        //or if it is just needed for both...
        try {
            int conNid = identifierProvider.getConceptSequenceForComponentNid(nid);
            if (conNid != Integer.MAX_VALUE)
            {
                ConceptChronicle concept = (ConceptChronicle)conceptProvider.getConcept(conNid);
                return Optional.ofNullable(concept.getComponent(nid));
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        //}
        return Optional.empty();
    }

    private static class CradleFetcher implements ConceptFetcherBI {

        ConceptChronicleDataEager eager;

        public CradleFetcher(ConceptChronicleDataEager eager) {
            this.eager = eager;
        }

        @Override
        public ConceptChronicleBI fetch() {
            return eager.getConceptChronicle();
        }

        @Override
        public Optional<ConceptVersionBI> fetch(ViewCoordinate vc) {
            return eager.getConceptChronicle().getVersion(vc);
        }

    }

    @Override
    public void iterateConceptDataInParallel(ProcessUnfetchedConceptDataBI processor) throws Exception {
        getParallelConceptDataEagerStream().forEach((concept) -> {
            try {
                processor.processUnfetchedConceptData(concept.getNid(), new CradleFetcher(concept));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    @Override
    public void iterateConceptDataInSequence(ProcessUnfetchedConceptDataBI processor) throws Exception {
        getConceptDataEagerStream().forEach((concept) -> {
            try {
                processor.processUnfetchedConceptData(concept.getNid(), new CradleFetcher(concept));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    @Override
    public boolean satisfiesDependencies(Collection<DbDependency> dbDependencies) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NativeIdSetBI getAllConceptNids() throws IOException {
        ConcurrentBitSet bitSet = new ConcurrentBitSet(getConceptCount());
        identifierProvider.getConceptSequenceStream().forEach((int sequence) -> {
            bitSet.set(identifierProvider.getConceptNid(sequence));
        });
        return bitSet;
    }

    @Override
    public NativeIdSetBI getAllConceptNidsFromCache() throws IOException {
        ConcurrentBitSet bitSet = new ConcurrentBitSet(getConceptCount());
        identifierProvider.getConceptSequenceStream().forEach((int sequence) -> {
            bitSet.set(identifierProvider.getConceptNid(sequence));
        });
        return bitSet;
    }

    @Override
    public NativeIdSetBI getAllComponentNids() throws IOException {
        return new IntSet(identifierProvider.getComponentNidStream().toArray());
    }

    @Override
    public NativeIdSetBI getComponentNidsForConceptNids(NativeIdSetBI conceptNidSet) throws IOException {
        NidSet results = identifierProvider.getComponentNidsForConceptNids(conceptNidSet.toConceptSequenceSet());
        return new IntSet(results.stream().toArray());
    }

    @Override
    public NativeIdSetBI getConceptNidsForComponentNids(NativeIdSetBI nativeIdSet) throws IOException {
        NativeIdSetBI conceptNids = new IntSet();
        IntStream.of(nativeIdSet.getSetValues()).forEach((componentNid) -> conceptNids.add(getConceptNidForNid(componentNid)));
        return conceptNids;
    }

    @Override
    public NativeIdSetBI getOrphanNids(NativeIdSetBI nativeIdSetBI) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getAuthorNidForStamp(int stamp) {
        return identifierProvider.getConceptNid(commitService.getAuthorSequenceForStamp(stamp));
    }

    @Override
    public NativeIdSetBI getEmptyNidSet() throws IOException {
        return new ConcurrentBitSet(getConceptCount());
    }

    @Override
    public int getModuleNidForStamp(int stamp) {
        return identifierProvider.getConceptNid(commitService.getModuleSequenceForStamp(stamp));
    }

    @Override
    public int getPathNidForStamp(int stamp) {
        return identifierProvider.getConceptNid(commitService.getPathSequenceForStamp(stamp));
    }

    @Override
    public Status getStatusForStamp(int stamp) {
        return Status.getStatusFromState(commitService.getStatusForStamp(stamp));
    }

    @Override
    public long getTimeForStamp(int stamp) {
        return commitService.getTimeForStamp(stamp);
    }

    @Override
    public int getNidForUuids(Collection<UUID> uuids) throws IOException {
        return getNidForUuids(uuids.toArray(new UUID[uuids.size()]));
    }

    /**
     * @see org.ihtsdo.otf.tcc.api.store.TerminologyDI#getConceptNidForNid(int)
     */
    @Override
    public int getConceptNidForNid(int nid) {
        return identifierProvider.getConceptNid(identifierProvider.getConceptSequenceForComponentNid(nid));
    }

    @Override
    public void writeConceptData(ConceptChronicleDataEager conceptData) {
        ((ConceptProviderOtfModel) conceptProvider).writeConceptData(conceptData);
    }

    @Override
    public RefexMember<?, ?> getRefex(int refexId) {
        return refexProvider.getRefex(refexId);
    }
    

    @Override
    public RefexDynamicChronicleBI<?> getDynamicRefex(int refexId)
    {
        return refexProvider.getRefexDynamic(refexId);
    }

	@Override
    public Collection<RefexMember<?, ?>> getRefexesForAssemblage(int assemblageNid) {
        return refexProvider.getRefexesFromAssemblage(assemblageNid).collect(Collectors.toList());
    }

    @Override
    public Collection<RefexMember<?, ?>> getRefexesForComponent(int componentNid) {
        return refexProvider.getRefexesForComponent(componentNid).collect(Collectors.toList());
    }

    @Override
    public int getStamp(Status status, long time, int authorNid, int moduleNid, int pathNid) {
        return commitService.getStampSequence(status.getState(),
                time,
                identifierProvider.getConceptSequence(authorNid),
                identifierProvider.getConceptSequence(moduleNid),
                identifierProvider.getConceptSequence(pathNid));

    }

    @Override
    public ConcurrentSkipListSet<DestinationOriginRecord> getDestinationOriginRecordSet() {
        return ((CradleTaxonomyProvider) taxonomyProvider).getDestinationOriginRecordSet();
    }

    @Override
    public CasSequenceObjectMap<TaxonomyRecordPrimitive> getOriginDestinationTaxonomyMap() {
        return ((CradleTaxonomyProvider) taxonomyProvider).getOriginDestinationTaxonomyRecords();
    }

    @Override
    public Task<Boolean> startVerifyTask(java.nio.file.Path... paths) {
        VerifyLoadEConceptFile loaderTask = VerifyLoadEConceptFile.create(paths, this);
        LookupService.getService(WorkExecutors.class).getForkJoinPoolExecutor().execute(loaderTask);
        return loaderTask;
    }

    @Override
    public ConceptChronicle getConcept(int cNid) {
        if (cNid >= 0) {
            return super.getConcept(identifierProvider.getConceptNid(cNid));
        }
        return super.getConcept(cNid);
    }

    @Override
    public Task<?> index(Class<?> ... indexersToReindex) {
        return startIndexTask(indexersToReindex);
    }

    /**
     * @see ObjectChronicleTaskService#startIndexTask(Class...)
     */
    @Override
    public GenerateIndexes startIndexTask(Class<?>... indexersToReindex) {
        GenerateIndexes indexingTask = new GenerateIndexes(indexersToReindex);
        LookupService.getService(WorkExecutors.class).getForkJoinPoolExecutor().execute(indexingTask);
        return indexingTask;
    }

    @Override
    public Task<Integer> startLoadTask(ConceptProxy stampPath, Path... filePaths) {
        ImportEConceptFile loaderTask = ImportEConceptFile.create(filePaths, this, stampPath);
        LookupService.getService(WorkExecutors.class).getForkJoinPoolExecutor().execute(loaderTask);
        return loaderTask;
    }

    @Override
    public Task<Boolean> startVerifyTask(ConceptProxy stampPath, Path... filePaths) {
        VerifyLoadEConceptFile loaderTask = VerifyLoadEConceptFile.create(filePaths, this, stampPath);
        LookupService.getService(WorkExecutors.class).getForkJoinPoolExecutor().execute(loaderTask);
        return loaderTask;

    }

    @Override
    public Task<Integer> startExportTask(ConceptProxy stampPath, Path filePath) {
        return ExportEConceptFile.create(filePath, this, ttkConceptChronicle -> {
            ttkConceptChronicle.processComponentRevisions(r -> {
                r.setPathUuid(stampPath.getUuids()[0]);
            });
        });
    }

    @Override
    public Task<Integer> startLogicGraphExportTask(ConceptProxy stampPath, Path filePath) {
        UUID pathUuid = stampPath.getUuids()[0];
        return ExportEConceptFile.create(filePath, this,
                (Consumer<TtkConceptChronicle>) (TtkConceptChronicle t) -> {
                    t.setRelationships(null);
                }, (Consumer<TtkConceptChronicle>) (TtkConceptChronicle t) -> {
                    t.processComponentRevisions(r -> {
                        r.setPathUuid(pathUuid);
                    });
                });
    }

    @Override
    public Task<Void> addStampPathOrigin(ConceptProxy stampPath, ConceptProxy originStampPath, Instant originTime) {
        AddStampOrigin addStampOrigin = new AddStampOrigin(stampPath, originStampPath, originTime, this);
        LookupService.getService(WorkExecutors.class).getForkJoinPoolExecutor().execute(addStampOrigin);
        return addStampOrigin;
    }

    @Override
    public void reportStats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Task<Integer> startLoadTask(StandardPaths stampPath, Path... filePaths) {
        switch (stampPath) {
            case DEVELOPMENT:
                return startLoadTask(IsaacMetadataAuxiliaryBinding.DEVELOPMENT, filePaths);
            case MASTER:
                return startLoadTask(IsaacMetadataAuxiliaryBinding.MASTER, filePaths);
            default:
                throw new UnsupportedOperationException("Can't handle: " + stampPath);
        }
    }

    @Override
    public Task<Boolean> startVerifyTask(StandardPaths stampPath, Path... filePaths) {
        switch (stampPath) {
            case DEVELOPMENT:
                return startVerifyTask(IsaacMetadataAuxiliaryBinding.DEVELOPMENT, filePaths);
            case MASTER:
                return startVerifyTask(IsaacMetadataAuxiliaryBinding.MASTER, filePaths);
            default:
                throw new UnsupportedOperationException("Can't handle: " + stampPath);
        }
    }

    @Override
    public Task<Integer> startExportTask(StandardPaths stampPath, Path filePath) {
        switch (stampPath) {
            case DEVELOPMENT:
                return startExportTask(IsaacMetadataAuxiliaryBinding.DEVELOPMENT, filePath);
            case MASTER:
                return startExportTask(IsaacMetadataAuxiliaryBinding.MASTER, filePath);
            default:
                throw new UnsupportedOperationException("Can't handle: " + stampPath);
        }
    }

    @Override
    public Task<Integer> startLogicGraphExportTask(StandardPaths stampPath, Path filePath) {
        switch (stampPath) {
            case DEVELOPMENT:
                return startLogicGraphExportTask(IsaacMetadataAuxiliaryBinding.DEVELOPMENT, filePath);
            case MASTER:
                return startLogicGraphExportTask(IsaacMetadataAuxiliaryBinding.MASTER, filePath);
            default:
                throw new UnsupportedOperationException("Can't handle: " + stampPath);
        }
    }

}
