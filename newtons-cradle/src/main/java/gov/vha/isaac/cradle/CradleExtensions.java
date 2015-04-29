package gov.vha.isaac.cradle;

import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordPrimitive;
import gov.vha.isaac.cradle.waitfree.CasSequenceObjectMap;
import gov.vha.isaac.cradle.component.ConceptChronicleDataEager;
import gov.vha.isaac.cradle.taxonomy.DestinationOriginRecord;
import gov.vha.isaac.ochre.collections.ConceptSequenceSet;
import java.util.concurrent.ConcurrentSkipListSet;
import org.ihtsdo.otf.tcc.model.cc.refex.RefexMember;
import org.ihtsdo.otf.tcc.model.cc.termstore.PersistentStoreI;
import org.jvnet.hk2.annotations.Contract;

import java.util.stream.Stream;

/**
 * Created by kec on 12/18/14.
 */
@Contract
public interface CradleExtensions extends PersistentStoreI {
    
    void writeConceptData(ConceptChronicleDataEager conceptData);

    Stream<ConceptChronicleDataEager> getConceptDataEagerStream();
    Stream<ConceptChronicleDataEager> getConceptDataEagerStream(ConceptSequenceSet conceptSequences);

    Stream<ConceptChronicleDataEager> getParallelConceptDataEagerStream();
    Stream<ConceptChronicleDataEager> getParallelConceptDataEagerStream(ConceptSequenceSet conceptSequences);

    ConcurrentSkipListSet<DestinationOriginRecord> getDestinationOriginRecordSet();
    CasSequenceObjectMap<TaxonomyRecordPrimitive> getOriginDestinationTaxonomyMap();

    void writeRefex(RefexMember<?, ?> sememe);
    
    Stream<RefexMember<?, ?>> getRefexStream();
    Stream<RefexMember<?, ?>> getParallelRefexStream();
    
    void reportStats();
}
