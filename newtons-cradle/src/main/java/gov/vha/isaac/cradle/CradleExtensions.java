package gov.vha.isaac.cradle;

import gov.vha.isaac.cradle.taxonomy.PrimitiveTaxonomyRecord;
import gov.vha.isaac.cradle.collections.CasSequenceObjectMap;
import gov.vha.isaac.cradle.component.ConceptChronicleDataEager;
import java.io.IOException;
import org.ihtsdo.otf.tcc.model.cc.concept.ConceptChronicle;
import org.ihtsdo.otf.tcc.model.cc.refex.RefexMember;
import org.ihtsdo.otf.tcc.model.cc.termstore.PersistentStoreI;
import org.jvnet.hk2.annotations.Contract;

import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Created by kec on 12/18/14.
 */
@Contract
public interface CradleExtensions extends PersistentStoreI {
    void writeConceptData(ConceptChronicleDataEager conceptData);

    Stream<ConceptChronicleDataEager> getConceptDataEagerStream();

    Stream<ConceptChronicleDataEager> getParallelConceptDataEagerStream();

    @Override
    Stream<ConceptChronicle> getConceptStream();

    @Override
    Stream<ConceptChronicle> getParallelConceptStream();

    CasSequenceObjectMap<PrimitiveTaxonomyRecord> getTaxonomyMap();

    int getConceptSequence(int nid);

    IntStream getConceptSequenceStream();

    IntStream getParallelConceptSequenceStream();

    void writeSememe(RefexMember<?, ?> sememe);

    void loadExistingDatabase() throws IOException;  
}
