/**
 * Copyright Notice
 *
 * This is a work of the U.S. Government and is not subject to copyright 
 * protection in the United States. Foreign copyrights may apply.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ihtsdo.otf.query.lucene;

import gov.vha.isaac.metadata.coordinates.ViewCoordinates;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.ihtsdo.otf.tcc.api.blueprint.IdDirective;
import org.ihtsdo.otf.tcc.api.blueprint.InvalidCAB;
import org.ihtsdo.otf.tcc.api.blueprint.RefexDirective;
import org.ihtsdo.otf.tcc.api.blueprint.RefexDynamicCAB;
import org.ihtsdo.otf.tcc.api.concept.ConceptChronicleBI;
import org.ihtsdo.otf.tcc.api.concept.ConceptVersionBI;
import org.ihtsdo.otf.tcc.api.contradiction.ContradictionException;
import org.ihtsdo.otf.tcc.api.coordinate.EditCoordinate;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.api.metadata.binding.RefexDynamic;
import org.ihtsdo.otf.tcc.api.refexDynamic.RefexDynamicChronicleBI;
import org.ihtsdo.otf.tcc.api.refexDynamic.RefexDynamicVersionBI;
import org.ihtsdo.otf.tcc.api.refexDynamic.data.RefexDynamicDataBI;
import org.ihtsdo.otf.tcc.api.refexDynamic.data.RefexDynamicDataType;
import org.ihtsdo.otf.tcc.api.refexDynamic.data.RefexDynamicUsageDescription;
import org.ihtsdo.otf.tcc.api.refexDynamic.data.dataTypes.RefexDynamicStringBI;
import org.ihtsdo.otf.tcc.api.spec.ValidationException;
import org.ihtsdo.otf.tcc.api.store.Ts;
import org.ihtsdo.otf.tcc.lookup.Hk2Looker;
import org.ihtsdo.otf.tcc.model.cc.refexDynamic.data.RefexDynamicData;
import org.ihtsdo.otf.tcc.model.cc.refexDynamic.data.dataTypes.RefexDynamicString;
import org.ihtsdo.otf.tcc.model.index.service.IndexStatusListenerBI;
import org.jvnet.hk2.annotations.Service;

/**
 * {@link LuceneDynamicRefexIndexerConfiguration} Holds a cache of the configuration for the dynamic refex indexer (which is read from the DB, and may
 * be changed at any point
 * the user wishes). Keeps track of which assemblage types need to be indexing, and what attributes should be indexed on them.
 *
 * @author <a href="mailto:daniel.armbrust.list@gmail.com">Dan Armbrust</a>
 */
@Service
public class LuceneDynamicRefexIndexerConfiguration
{
	private static final Logger logger = Logger.getLogger(LuceneDynamicRefexIndexer.class.getName());

	//store assemblage nids that should be indexed - and then - for COLUMN_DATA keys, keep the 0 indexed column order numbers that need to be indexed.
	private HashMap<Integer, Integer[]> whatToIndex_ = new HashMap<>();

	private volatile boolean readNeeded_ = true;

	protected boolean needsIndexing(int assemblageNid)
	{
		initCheck();
		return whatToIndex_.containsKey(assemblageNid);
	}

	protected Integer[] whatColumnsToIndex(int assemblageNid)
	{
		initCheck();
		return whatToIndex_.get(assemblageNid);
	}

	private void initCheck()
	{
		if (readNeeded_)
		{
			logger.fine("Reading Dynamic Refex Index Configuration");
			try
			{
				HashMap<Integer, Integer[]> updatedWhatToIndex = new HashMap<>();

				ConceptVersionBI c = Ts.get().getConceptVersion(ViewCoordinates.getMetadataViewCoordinate(), RefexDynamic.DYNAMIC_SEMEME_INDEX_CONFIGURATION.getUuids()[0]);

				for (RefexDynamicChronicleBI<?> r : c.getRefsetDynamicMembers())
				{
					RefexDynamicVersionBI<?> rdv = r.getVersion(ViewCoordinates.getMetadataViewCoordinate());
					if (rdv == null || !rdv.isActive() || rdv.getAssemblageNid() != RefexDynamic.DYNAMIC_SEMEME_INDEX_CONFIGURATION.getNid())
					{
						continue;
					}
					int assemblageToIndex = rdv.getReferencedComponentNid();
					Integer[] finalCols = new Integer[] {};
					RefexDynamicDataBI[] data = rdv.getData();
					if (data != null && data.length > 0)
					{
						String colsToIndex = ((RefexDynamicStringBI) data[0]).getDataString();
						String[] split = colsToIndex.split(",");
						finalCols = new Integer[split.length];
						for (int i = 0; i < split.length; i++)
						{
							finalCols[i] = Integer.parseInt(split[i]);
						}
					}
					else
					{
						//make sure it is an annotation style if the col list is empty
						if (!RefexDynamicUsageDescription.read(assemblageToIndex).isAnnotationStyle())
						{
							logger.warning("Dynamic Refex Indexer was configured to index a member style refex, but no columns were specified.  Skipping.");
							continue;
						}
					}
					updatedWhatToIndex.put(assemblageToIndex, finalCols);
				}

				whatToIndex_ = updatedWhatToIndex;
				readNeeded_ = false;
			}
			catch (Exception e)
			{
				logger.log(Level.SEVERE, "Unexpected error reading Dynamic Refex Index Configuration - generated index will be incomplete!", e);
			}
		}
	}

	/**
	 * for the given assemblage nid, which columns should be indexed? null or empty list for none.
	 * otherwise, 0 indexed column numbers.
	 * 
	 * Note - columnsToIndex must be provided for member-style assemblage NIDs - it doesn't make any
	 * sense to index the assemblageID of a member style refex.
	 * 
	 * So, for member style - you can configure which columns to index.
	 * For annotation style - you can configure just indexing the assemblage itself, or you can also
	 * index individual data columns.
	 * 
	 * @param skipReindex - if true - does not do a full DB reindex (useful if you are enabling an index on a new refex that has never been used)
	 * otherwise - leave false - so that a full reindex occurs (on this thread) and the index becomes valid.
	 * 
	 * @throws IOException
	 * @throws InvalidCAB
	 * @throws ContradictionException
	 */
	public static void configureColumnsToIndex(int assemblageNid, Integer[] columnsToIndex, boolean skipReindex) throws ContradictionException, InvalidCAB, IOException
	{
		Hk2Looker.get().getService(LuceneDynamicRefexIndexerConfiguration.class).readNeeded_ = true;
		List<IndexStatusListenerBI> islList = Hk2Looker.get().getAllServices(IndexStatusListenerBI.class);
		for (IndexStatusListenerBI isl : islList)
		{
			isl.indexConfigurationChanged(Hk2Looker.get().getService(LuceneDynamicRefexIndexer.class));
		}

		ConceptChronicleBI referencedAssemblageConceptC = Ts.get().getConcept(assemblageNid);
		
		ConceptVersionBI assemblageConceptC = Ts.get().getConceptVersion(ViewCoordinates.getMetadataViewCoordinate(),
				RefexDynamic.DYNAMIC_SEMEME_INDEX_CONFIGURATION.getNid());
		
		logger.info("Configuring index for dynamic refex assemblage '" + referencedAssemblageConceptC.toUserString() + "' on columns " + Arrays.deepToString(columnsToIndex));

		StringBuilder buf = new StringBuilder();
		RefexDynamicData[] data = null;
		if (columnsToIndex != null)
		{
			for (int i : columnsToIndex)
			{
				buf.append(i);
				buf.append(",");
			}
			if (buf.length() > 0)
			{
				buf.setLength(buf.length() - 1);
			}

			if (buf.length() > 0)
			{
				data = new RefexDynamicData[1];

				try
				{
					data[0] = new RefexDynamicString(buf.toString());
				}
				catch (PropertyVetoException e)
				{
					throw new RuntimeException("Shoudn't be possible");
				}
			}
		}
		else if ((columnsToIndex == null || columnsToIndex.length == 0) && !assemblageConceptC.isAnnotationStyleRefex())
		{
			throw new IOException("It doesn't make sense to index a member-style refex (without indexing any column data)");
		}

		RefexDynamicVersionBI<?> rdv = findCurrentIndexConfigRefex(assemblageNid);
		
		RefexDynamicCAB rdb = null;
		
		if (rdv != null)
		{
			rdb = rdv.makeBlueprint(ViewCoordinates.getMetadataViewCoordinate(), IdDirective.PRESERVE, RefexDirective.EXCLUDE);
		}
		else
		{
			rdb = new RefexDynamicCAB(assemblageNid, RefexDynamic.DYNAMIC_SEMEME_INDEX_CONFIGURATION.getNid());
		}
		rdb.setData(data, null);
		
		Ts.get().getTerminologyBuilder(new EditCoordinate(IsaacMetadataAuxiliaryBinding.USER.getLenient().getConceptNid(), 
				IsaacMetadataAuxiliaryBinding.ISAAC_MODULE.getLenient().getNid(), 
				IsaacMetadataAuxiliaryBinding.MASTER.getLenient().getConceptNid()),
				ViewCoordinates.getMetadataViewCoordinate()).construct(rdb);
		
		Ts.get().addUncommitted(assemblageConceptC);
		Ts.get().addUncommitted(referencedAssemblageConceptC);
		Ts.get().commit(/* assemblageConceptC */);
		Ts.get().commit(/* referencedAssemblageConceptC */);
		if (!skipReindex)
		{
			Ts.get().index(new Class[] {LuceneDynamicRefexIndexer.class});
		}
	}
	
	/**
	 * Read the indexing configuration for the specified dynamic refex.
	 * 
	 * Returns null, if the assemblage is not indexed at all.  Returns an empty array, if the assemblage is indexed (but no columns are indexed)
	 * Returns an integer array of the column positions of the refex that are indexed, if any.
	 * 
	 */
	public static Integer[] readIndexInfo(int assemblageNid) throws IOException, ContradictionException
	{
		return Hk2Looker.get().getService(LuceneDynamicRefexIndexerConfiguration.class).whatColumnsToIndex(assemblageNid);
	}
	
	private static RefexDynamicVersionBI<?> findCurrentIndexConfigRefex(int assemblageNid) throws ValidationException, IOException, ContradictionException
	{
		ConceptVersionBI indexConfigConcept = Ts.get().getConceptVersion(ViewCoordinates.getMetadataViewCoordinate(),
				RefexDynamic.DYNAMIC_SEMEME_INDEX_CONFIGURATION.getNid());

		for (RefexDynamicChronicleBI<?> r : indexConfigConcept.getRefsetDynamicMembers())
		{
			if (r.getAssemblageNid() == RefexDynamic.DYNAMIC_SEMEME_INDEX_CONFIGURATION.getNid() && r.getReferencedComponentNid() == assemblageNid)
			{
				RefexDynamicVersionBI<?> rdv = r.getVersion(ViewCoordinates.getMetadataViewCoordinate());
				
				if (rdv != null && rdv.getAssemblageNid() == RefexDynamic.DYNAMIC_SEMEME_INDEX_CONFIGURATION.getNid() && rdv.getReferencedComponentNid() == assemblageNid)
				{
					return rdv;
				}
			}
		}
		return null;
	}

	/**
	 * Disable all indexing of the specified refex.  To change the index config, use the {@link #configureColumnsToIndex(int, Integer[]) method.
	 * 
	 * @throws IOException 	 
	 * @throws ContradictionException 
	 * @throws InvalidCAB */
	public static void disableIndex(int assemblageNid) throws IOException, InvalidCAB, ContradictionException
	{
		logger.info("Disabling index for dynamic refex assemblage concept '" + assemblageNid + "'");
		
		RefexDynamicVersionBI<?> rdv = findCurrentIndexConfigRefex(assemblageNid);
		
		if (rdv != null && rdv.isActive())
		{
			Hk2Looker.get().getService(LuceneDynamicRefexIndexerConfiguration.class).readNeeded_ = true;
			List<IndexStatusListenerBI> islList = Hk2Looker.get().getAllServices(IndexStatusListenerBI.class);
			for (IndexStatusListenerBI isl : islList)
			{
				isl.indexConfigurationChanged(Hk2Looker.get().getService(LuceneDynamicRefexIndexer.class));
			}
			RefexDynamicCAB rb = rdv.makeBlueprint(ViewCoordinates.getMetadataViewCoordinate(), IdDirective.PRESERVE, RefexDirective.EXCLUDE);
			rb.setStatus(Status.INACTIVE);
			ConceptVersionBI indexConfigConceptC = Ts.get().getConceptVersion(ViewCoordinates.getMetadataViewCoordinate(),
					RefexDynamic.DYNAMIC_SEMEME_INDEX_CONFIGURATION.getNid());
			ConceptChronicleBI referencedAssemblageConceptC = Ts.get().getConcept(assemblageNid);
			
			Ts.get().getTerminologyBuilder(new EditCoordinate(IsaacMetadataAuxiliaryBinding.USER.getLenient().getConceptNid(), 
					IsaacMetadataAuxiliaryBinding.ISAAC_MODULE.getLenient().getNid(), 
					IsaacMetadataAuxiliaryBinding.MASTER.getLenient().getConceptNid()),
					ViewCoordinates.getMetadataViewCoordinate()).construct(rb);

			Ts.get().addUncommitted(indexConfigConceptC);
			Ts.get().addUncommitted(referencedAssemblageConceptC);
			Ts.get().commit(/* indexConfigConceptC */);
			Ts.get().commit(/* referencedAssemblageConceptC */);
			Ts.get().index(new Class[] {LuceneDynamicRefexIndexer.class});
			return;
		}
		logger.info("No index configuration was found to disable for dynamic refex assemblage concept '" + assemblageNid + "'");
	}
	
	public static boolean isColumnTypeIndexable(RefexDynamicDataType dataType)
	{
		if (dataType == RefexDynamicDataType.BYTEARRAY)
		{
			return false;
		}
		return true;
	}
	
	public static boolean isAssemblageIndexed(int assemblageNid)
	{
		return Hk2Looker.get().getService(LuceneDynamicRefexIndexerConfiguration.class).needsIndexing(assemblageNid);
	}
}
