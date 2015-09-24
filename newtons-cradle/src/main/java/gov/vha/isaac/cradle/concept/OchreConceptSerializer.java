package gov.vha.isaac.cradle.concept;

import gov.vha.isaac.cradle.taxonomy.TaxonomyFlags;
import gov.vha.isaac.cradle.taxonomy.TaxonomyRecordPrimitive;
import gov.vha.isaac.cradle.waitfree.CasSequenceObjectMap;
import gov.vha.isaac.cradle.waitfree.WaitFreeMergeSerializer;
import gov.vha.isaac.ochre.model.DataBuffer;
import gov.vha.isaac.ochre.model.concept.ConceptChronologyImpl;

/**
 * Created by kec on 5/15/15.
 */
public class OchreConceptSerializer implements WaitFreeMergeSerializer<ConceptChronologyImpl> {

	CasSequenceObjectMap<TaxonomyRecordPrimitive> originDestinationTaxonomyRecords;

	public OchreConceptSerializer(CasSequenceObjectMap<TaxonomyRecordPrimitive> originDestinationTaxonomyRecords) {
		this.originDestinationTaxonomyRecords = originDestinationTaxonomyRecords;
	}

	@Override
	public void serialize(DataBuffer d, ConceptChronologyImpl a) {
		int conceptSequence = a.getConceptSequence();
		TaxonomyRecordPrimitive parentTaxonomyRecord;
		if (originDestinationTaxonomyRecords.containsKey(conceptSequence)) {
			parentTaxonomyRecord = originDestinationTaxonomyRecords.get(conceptSequence).get();
		} else {
			parentTaxonomyRecord = new TaxonomyRecordPrimitive();
		}

		a.getVersionStampSequences().forEach((stampSequence) -> {
			parentTaxonomyRecord.getTaxonomyRecordUnpacked().addStampRecord(conceptSequence,
					  conceptSequence, stampSequence, TaxonomyFlags.CONCEPT_STATUS.bits);
		});

		originDestinationTaxonomyRecords.put(conceptSequence, parentTaxonomyRecord);

		byte[] data = a.getDataToWrite();
		d.put(data, 0, data.length);
	}

	@Override
	public ConceptChronologyImpl merge(ConceptChronologyImpl a, ConceptChronologyImpl b, int writeSequence) {
		byte[] dataBytes = a.mergeData(writeSequence, b.getDataToWrite(writeSequence));
		DataBuffer db = new DataBuffer(dataBytes);
		return new ConceptChronologyImpl(db);
	}

	@Override
	public ConceptChronologyImpl deserialize(DataBuffer db) {
		return new ConceptChronologyImpl(db);
	}

}
