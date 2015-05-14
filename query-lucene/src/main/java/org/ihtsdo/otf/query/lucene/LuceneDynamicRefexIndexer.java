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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ihtsdo.otf.query.lucene;

import gov.vha.isaac.ochre.api.component.sememe.SememeChronology;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.glassfish.hk2.runlevel.RunLevel;
import org.ihtsdo.otf.tcc.api.blueprint.ComponentProperty;
import org.ihtsdo.otf.tcc.api.chronicle.ComponentChronicleBI;
import org.ihtsdo.otf.tcc.api.refexDynamic.RefexDynamicChronicleBI;
import org.ihtsdo.otf.tcc.api.refexDynamic.RefexDynamicVersionBI;
import org.ihtsdo.otf.tcc.api.refexDynamic.data.RefexDynamicDataBI;
import org.ihtsdo.otf.tcc.api.refexDynamic.data.dataTypes.RefexDynamicArrayBI;
import org.ihtsdo.otf.tcc.api.refexDynamic.data.dataTypes.RefexDynamicBooleanBI;
import org.ihtsdo.otf.tcc.api.refexDynamic.data.dataTypes.RefexDynamicByteArrayBI;
import org.ihtsdo.otf.tcc.api.refexDynamic.data.dataTypes.RefexDynamicDoubleBI;
import org.ihtsdo.otf.tcc.api.refexDynamic.data.dataTypes.RefexDynamicFloatBI;
import org.ihtsdo.otf.tcc.api.refexDynamic.data.dataTypes.RefexDynamicIntegerBI;
import org.ihtsdo.otf.tcc.api.refexDynamic.data.dataTypes.RefexDynamicLongBI;
import org.ihtsdo.otf.tcc.api.refexDynamic.data.dataTypes.RefexDynamicNidBI;
import org.ihtsdo.otf.tcc.api.refexDynamic.data.dataTypes.RefexDynamicPolymorphicBI;
import org.ihtsdo.otf.tcc.api.refexDynamic.data.dataTypes.RefexDynamicStringBI;
import org.ihtsdo.otf.tcc.api.refexDynamic.data.dataTypes.RefexDynamicUUIDBI;
import org.ihtsdo.otf.tcc.model.cc.refexDynamic.data.dataTypes.RefexDynamicString;
import org.ihtsdo.otf.tcc.model.index.service.SearchResult;
import org.jvnet.hk2.annotations.Service;

/**
 * {@link LuceneDynamicRefexIndexer} An indexer that can be used both to index the reverse mapping of annotation style refexes, and to index
 * the attached data of dynamic refexes.
 *
 * This class provides specialized query methods for handling very specific queries against RefexDynamic data.
 *
 * @author <a href="mailto:daniel.armbrust.list@gmail.com">Dan Armbrust</a>
 */

@Service(name = "Dynamic Refex indexer")
@RunLevel(value = 2)
public class LuceneDynamicRefexIndexer extends LuceneIndexer
{
	private final Logger logger = Logger.getLogger(LuceneDynamicRefexIndexer.class.getName());
	
	public static final String INDEX_NAME = "dynamicRefex";
	private static final String COLUMN_FIELD_DATA = "colData";
	protected static final String COLUMN_FIELD_ASSEMBLAGE = "assemblage";

	@Inject 
	private LuceneDynamicRefexIndexerConfiguration lric;

	private LuceneDynamicRefexIndexer() throws IOException
	{
		//For HK2
		super(INDEX_NAME);
	}
	
	@PostConstruct
	private void startMe() throws IOException {
		logger.info("Starting Lucene Dynamic Refex indexer - post-construct");
		
	}
	
	//TODO dan hacking Keith question - if are these required, or not?  Can we put this in the parent class?  Or should we make abstract?
	//shouldn't be a thing we have to remember to implement.
	@PreDestroy
	private void stopMe() throws IOException {
		logger.info("Stopping Lucene Dynamic Refex indexer - pre-destroy. ");
		commitWriter();
		closeWriter();
	}
	
	@Override
	protected boolean indexSememeChronicle(SememeChronology<?> chronicle) {
		return false;
	}

	@Override
	protected void addFields(SememeChronology<?> chronicle, Document doc) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	protected boolean indexChronicle(ComponentChronicleBI<?> chronicle)
	{
		if (chronicle instanceof RefexDynamicChronicleBI)
		{
			RefexDynamicChronicleBI<?> rdc = (RefexDynamicChronicleBI<?>) chronicle;
			return lric.needsIndexing(rdc.getAssemblageNid());
		}

		return false;
	}

	@Override
	protected void addFields(ComponentChronicleBI<?> chronicle, Document doc)
	{
		RefexDynamicChronicleBI<?> rdc = (RefexDynamicChronicleBI<?>) chronicle;
		for (@SuppressWarnings("unchecked") Iterator<RefexDynamicVersionBI<?>> it = (Iterator<RefexDynamicVersionBI<?>>) rdc.getVersions().iterator(); it.hasNext();)
		{
			RefexDynamicVersionBI<?> rdv = it.next();
			
			//Yes, this is a long, but we never do anything other than exact matches, so it performs better to index it as a string
			//rather that indexing it as a long... as we never need to match things like nid > X
			doc.add(new StringField(COLUMN_FIELD_ASSEMBLAGE, rdv.getAssemblageNid() + "", Store.NO));
			
			Integer[] columns = lric.whatColumnsToIndex(rdv.getAssemblageNid());
			if (columns != null)
			{
				int dataColCount = rdv.getData().length;
				for (int col : columns)
				{
					RefexDynamicDataBI dataCol = col >= dataColCount ? null : rdv.getData(col);
					
					//Not the greatest design for diskspace / performance... but if we want to be able to support searching across 
					//all fields / all refexes - and also support searching per-field within a single refex, we need to double index 
					//all of the data.  Once with a standard field name, and once with a field name that includes the column number.
					//at search time, restricting to certain field matches is only allowed if they are also restricting to an assemblage,
					//so we can compute the correct field number list at search time.
					
					//the cheaper option from a disk space perspective (maybe, depending on the data) would be to create a document per 
					//column.  The queries would be trivial to write then, but we would be duplicating the component nid and assemblage nid
					//in each document, which is also expensive.  It also doesn't fit the model in OTF, of a document per component.
					
					//We also duplicate again, on string fields by indexing with the white space analyzer, in addition to the normal one.

					handleType(doc, dataCol, col, rdv);
				}
			}
		}
	}
	
	private void handleType(Document doc, RefexDynamicDataBI dataCol, int colNumber, RefexDynamicVersionBI<?> rdv)
	{
		if (dataCol == null)
		{
			//noop
		}
		else if (dataCol instanceof RefexDynamicBooleanBI)
		{
			doc.add(new StringField(COLUMN_FIELD_DATA, ((RefexDynamicBooleanBI) dataCol).getDataBoolean() + "", Store.NO));
			doc.add(new StringField(COLUMN_FIELD_DATA + "_" + colNumber, ((RefexDynamicBooleanBI) dataCol).getDataBoolean() + "", Store.NO));
		}
		else if (dataCol instanceof RefexDynamicByteArrayBI)
		{
			logger.warning("Dynamic Refex Indexer configured to index a field that isn't indexable (byte array) in " + rdv.toUserString());
		}
		else if (dataCol instanceof RefexDynamicDoubleBI)
		{
			doc.add(new DoubleField(COLUMN_FIELD_DATA, ((RefexDynamicDoubleBI) dataCol).getDataDouble(), Store.NO));
			doc.add(new DoubleField(COLUMN_FIELD_DATA + "_" + colNumber, ((RefexDynamicDoubleBI) dataCol).getDataDouble(), Store.NO));
		}
		else if (dataCol instanceof RefexDynamicFloatBI)
		{
			doc.add(new FloatField(COLUMN_FIELD_DATA, ((RefexDynamicFloatBI) dataCol).getDataFloat(), Store.NO));
			doc.add(new FloatField(COLUMN_FIELD_DATA + "_" + colNumber, ((RefexDynamicFloatBI) dataCol).getDataFloat(), Store.NO));
		}
		else if (dataCol instanceof RefexDynamicIntegerBI)
		{
			doc.add(new IntField(COLUMN_FIELD_DATA, ((RefexDynamicIntegerBI) dataCol).getDataInteger(), Store.NO));
			doc.add(new IntField(COLUMN_FIELD_DATA + "_" + colNumber, ((RefexDynamicIntegerBI) dataCol).getDataInteger(), Store.NO));
		}
		else if (dataCol instanceof RefexDynamicLongBI)
		{
			doc.add(new LongField(COLUMN_FIELD_DATA, ((RefexDynamicLongBI) dataCol).getDataLong(), Store.NO));
			doc.add(new LongField(COLUMN_FIELD_DATA + "_" + colNumber, ((RefexDynamicLongBI) dataCol).getDataLong(), Store.NO));
		}
		else if (dataCol instanceof RefexDynamicNidBI)
		{
			//No need for ranges on a nid
			doc.add(new StringField(COLUMN_FIELD_DATA, ((RefexDynamicNidBI) dataCol).getDataNid() + "", Store.NO));
			doc.add(new StringField(COLUMN_FIELD_DATA + "_" + colNumber, ((RefexDynamicNidBI) dataCol).getDataNid() + "", Store.NO));
		}
		else if (dataCol instanceof RefexDynamicPolymorphicBI)
		{
			logger.log(Level.SEVERE, "This should have been impossible (polymorphic?)");
		}
		else if (dataCol instanceof RefexDynamicStringBI)
		{
			doc.add(new TextField(COLUMN_FIELD_DATA, ((RefexDynamicStringBI) dataCol).getDataString(), Store.NO));
			doc.add(new TextField(COLUMN_FIELD_DATA + "_" + colNumber, ((RefexDynamicStringBI) dataCol).getDataString(), Store.NO));
			//yes, indexed 4 different times - twice with the standard analyzer, twice with the whitespace analyzer.
			doc.add(new TextField(COLUMN_FIELD_DATA + PerFieldAnalyzer.WHITE_SPACE_FIELD_MARKER, ((RefexDynamicStringBI) dataCol).getDataString(), Store.NO));
			doc.add(new TextField(COLUMN_FIELD_DATA + "_" + colNumber + PerFieldAnalyzer.WHITE_SPACE_FIELD_MARKER, 
					((RefexDynamicStringBI) dataCol).getDataString(), Store.NO));
		}
		else if (dataCol instanceof RefexDynamicUUIDBI)
		{
			//Use the whitespace analyzer on UUIDs
			doc.add(new StringField(COLUMN_FIELD_DATA + PerFieldAnalyzer.WHITE_SPACE_FIELD_MARKER, ((RefexDynamicUUIDBI) dataCol).getDataUUID().toString(), Store.NO));
			doc.add(new StringField(COLUMN_FIELD_DATA + "_" + colNumber + PerFieldAnalyzer.WHITE_SPACE_FIELD_MARKER, 
					((RefexDynamicUUIDBI) dataCol).getDataUUID().toString(), Store.NO));
		}
		else if (dataCol instanceof RefexDynamicArrayBI<?>)
		{
			for (RefexDynamicDataBI nestedData : ((RefexDynamicArrayBI<?>)dataCol).getDataArray())
			{
				handleType(doc, nestedData, colNumber, rdv);
			}
		}
		else
		{
			logger.log(Level.SEVERE, "This should have been impossible (no match on col type) " + dataCol);
		}
	}

	/**
	 * @param queryDataLower
	 * @param queryDataLowerInclusive
	 * @param queryDataUpper
	 * @param queryDataUpperInclusive
	 * @param assemblageNid (optional) limit the search to the specified assemblage
	 * @param searchColumns (optional) limit the search to the specified columns of attached data
	 * @param sizeLimit
	 * @param targetGeneration (optional) wait for an index to build, or null to not wait
	 */
	public final List<SearchResult> queryNumericRange(final RefexDynamicDataBI queryDataLower, final boolean queryDataLowerInclusive, 
			final RefexDynamicDataBI queryDataUpper, final boolean queryDataUpperInclusive, Integer assemblageNid, Integer[] searchColumns, 
			int sizeLimit, Long targetGeneration) throws IOException, ParseException
	{
		BooleanQuery bq = new BooleanQuery();

		if (assemblageNid != null)
		{
			bq.add(new TermQuery(new Term(COLUMN_FIELD_ASSEMBLAGE, assemblageNid + "")), Occur.MUST);
		}

		bq.add(new QueryWrapperForColumnHandling()
		{
			@Override
			Query buildQuery(String columnName) throws ParseException
			{
				return buildNumericQuery(queryDataLower, queryDataLowerInclusive, queryDataUpper, queryDataUpperInclusive, columnName);
			}
		}.buildColumnHandlingQuery(searchColumns), Occur.MUST);

		return search(bq, sizeLimit, targetGeneration);
	}

	/**
	 * A convenience method.
	 * 
	 * Search RefexDynamicData columns, treating them as text - and handling the search in the same mechanism as if this were a
	 * call to the method {@link LuceneIndexer#query(String, boolean, ComponentProperty, int, long)} using a ComponentProperty type
	 * of {@link ComponentProperty#DESCRIPTION_TEXT}
	 * 
	 * Calls the method {@link #query(RefexDynamicDataBI, Integer, boolean, Integer[], int, long) with a null parameter for
	 * the searchColumns, and wraps the queryString into a RefexDynamicString.
	 */
	public final List<SearchResult> query(String queryString, Integer assemblageNid, boolean prefixSearch, int sizeLimit, Long targetGeneration) throws IOException,
			ParseException
	{
		try
		{
			return query(new RefexDynamicString(queryString), assemblageNid, prefixSearch, null, sizeLimit, targetGeneration);
		}
		catch (PropertyVetoException e)
		{
			throw new ParseException(e.getMessage());
		}
	}

	/**
	 * 
	 * @param queryData - The query data object (string, int, etc)
	 * @param assemblageNid (optional) limit the search to the specified assemblage
	 * @param prefixSearch see {@link LuceneIndexer#query(String, boolean, ComponentProperty, int, Long)} for a description.
	 * @param searchColumns (optional) limit the search to the specified columns of attached data
	 * @param sizeLimit
	 * @param targetGeneration (optional) wait for an index to build, or null to not wait
	 */
	public final List<SearchResult> query(final RefexDynamicDataBI queryData, Integer assemblageNid, final boolean prefixSearch, Integer[] searchColumns, int sizeLimit,
			Long targetGeneration) throws IOException, ParseException
	{
		BooleanQuery bq = new BooleanQuery();
		if (assemblageNid != null)
		{
			bq.add(new TermQuery(new Term(COLUMN_FIELD_ASSEMBLAGE, assemblageNid + "")), Occur.MUST);
		}

		if (queryData instanceof RefexDynamicStringBI)
		{
			bq.add(new QueryWrapperForColumnHandling()
			{
				@Override
				Query buildQuery(String columnName) throws ParseException, IOException
				{
					//This is the only query type that needs tokenizing, etc.
					String queryString = ((RefexDynamicStringBI) queryData).getDataString();
					//'-' signs are operators to lucene... but we want to allow nid lookups.  So escape any leading hyphens
					//and any hyphens that are preceeded by spaces.  This way, we don't mess up UUID handling.
					//(lucene handles UUIDs ok, because the - sign is only treated special at the beginning, or when preceeded by a space)
					
					if (queryString.startsWith("-"))
					{
						queryString = "\\" + queryString;
					}
					queryString = queryString.replaceAll("\\s-", " \\\\-");
					logger.fine("Modified search string is: '" + queryString + "'");
					return buildTokenizedStringQuery(queryString, columnName, prefixSearch);
				}
			}.buildColumnHandlingQuery(searchColumns), Occur.MUST);
		}
		else
		{
			if (queryData instanceof RefexDynamicBooleanBI || queryData instanceof RefexDynamicNidBI || queryData instanceof RefexDynamicUUIDBI)
			{
				bq.add(new QueryWrapperForColumnHandling()
				{
					@Override
					Query buildQuery(String columnName) throws ParseException
					{
						return new TermQuery(new Term(columnName, queryData.getDataObject().toString()));
					}
				}.buildColumnHandlingQuery(searchColumns), Occur.MUST);
			}
			else if (queryData instanceof RefexDynamicDoubleBI || queryData instanceof RefexDynamicFloatBI || queryData instanceof RefexDynamicIntegerBI
					|| queryData instanceof RefexDynamicLongBI)
			{
				bq.add(new QueryWrapperForColumnHandling()
				{
					@Override
					Query buildQuery(String columnName) throws ParseException
					{
						Query temp = buildNumericQuery(queryData, true, queryData, true, columnName);
						
						if ((queryData instanceof RefexDynamicLongBI && ((RefexDynamicLongBI)queryData).getDataLong() < 0) ||
								(queryData instanceof RefexDynamicIntegerBI && ((RefexDynamicIntegerBI)queryData).getDataInteger() < 0))
						{
							//Looks like a nid... wrap in an or clause that would do a match on the exact term if it was indexed as a nid, rather than a numeric
							BooleanQuery wrapper = new BooleanQuery();
							wrapper.add(new TermQuery(new Term(columnName, queryData.getDataObject().toString())), Occur.SHOULD);
							wrapper.add(temp, Occur.SHOULD);
							temp = wrapper;
						}
						return temp;
					}
				}.buildColumnHandlingQuery(searchColumns), Occur.MUST);
			}
			else if (queryData instanceof RefexDynamicByteArrayBI)
			{
				throw new ParseException("RefexDynamicByteArray isn't indexed");
			}
			else if (queryData instanceof RefexDynamicPolymorphicBI)
			{
				throw new ParseException("This should have been impossible (polymorphic?)");
			}
			else
			{
				logger.log(Level.SEVERE, "This should have been impossible (no match on col type)");
				throw new ParseException("unexpected error, see logs");
			}
		}
		return search(bq, sizeLimit, targetGeneration);
	}

	/**
	 * A convenience method that calls:
	 * 
	 * {@link LuceneIndexer#query(String, boolean, ComponentProperty, int, Long) with:
	 * "boolean prefexSearch" set to false "ComponentProperty field" set to {@link ComponentProperty#ASSEMBLAGE_ID}
	 */
	public final List<SearchResult> queryAssemblageUsage(int assemblageNid, int sizeLimit, Long targetGeneration) throws IOException, ParseException, NumberFormatException
	{
		return query(assemblageNid + "", false, ComponentProperty.ASSEMBLAGE_ID, sizeLimit, targetGeneration);
	}

	private Query buildNumericQuery(RefexDynamicDataBI queryDataLower, boolean queryDataLowerInclusive, RefexDynamicDataBI queryDataUpper, boolean queryDataUpperInclusive,
			String columnName) throws ParseException
	{
		//Convert both to the same type (if they differ) - go largest data type to smallest, so we don't lose precision
		//Also - if they pass in longs that would fit in an int, also generate an int query.
		//likewise, with Double - if they pass in a double, that would fit in a float, also generate a float query.
		try
		{
			BooleanQuery bq = new BooleanQuery();
			boolean fitsInFloat = false;
			boolean fitsInInt = false;
			if (queryDataLower instanceof RefexDynamicDoubleBI || queryDataUpper instanceof RefexDynamicDoubleBI)
			{
				Double upperVal = (queryDataUpper == null ? null : (queryDataUpper instanceof RefexDynamicDoubleBI ? ((RefexDynamicDoubleBI)queryDataUpper).getDataDouble() :
					((Number)queryDataUpper.getDataObject()).doubleValue()));
				Double lowerVal = (queryDataLower == null ? null : (queryDataLower instanceof RefexDynamicDoubleBI ? ((RefexDynamicDoubleBI)queryDataLower).getDataDouble() :
					((Number)queryDataLower.getDataObject()).doubleValue()));
				bq.add(NumericRangeQuery.newDoubleRange(columnName, lowerVal, upperVal, queryDataLowerInclusive, queryDataUpperInclusive), Occur.SHOULD);
				
				if ((upperVal != null && upperVal <= Float.MAX_VALUE && upperVal >= Float.MIN_VALUE) 
						|| (lowerVal != null && lowerVal <= Float.MAX_VALUE && lowerVal >= Float.MIN_VALUE))
				{
					fitsInFloat = true;
				}
			}
			
			if (fitsInFloat || queryDataLower instanceof RefexDynamicFloatBI || queryDataUpper instanceof RefexDynamicFloatBI)
			{
				Float upperVal = (queryDataUpper == null ? null : 
					(queryDataUpper == null ? null : (queryDataUpper instanceof RefexDynamicFloatBI ? ((RefexDynamicFloatBI)queryDataUpper).getDataFloat() :
						(fitsInFloat && ((Number)queryDataUpper.getDataObject()).doubleValue() > Float.MAX_VALUE ? Float.MAX_VALUE : 
							((Number)queryDataUpper.getDataObject()).floatValue()))));
				Float lowerVal = (queryDataLower == null ? null : 
					(queryDataLower instanceof RefexDynamicFloatBI ? ((RefexDynamicFloatBI)queryDataLower).getDataFloat() :
						(fitsInFloat && ((Number)queryDataLower.getDataObject()).doubleValue() < Float.MIN_VALUE ? Float.MIN_VALUE :
							((Number)queryDataLower.getDataObject()).floatValue())));
				bq.add(NumericRangeQuery.newFloatRange(columnName, lowerVal, upperVal, queryDataLowerInclusive, queryDataUpperInclusive), Occur.SHOULD);
			}
			
			if (queryDataLower instanceof RefexDynamicLongBI || queryDataUpper instanceof RefexDynamicLongBI)
			{
				Long upperVal = (queryDataUpper == null ? null : (queryDataUpper instanceof RefexDynamicLongBI ? ((RefexDynamicLongBI)queryDataUpper).getDataLong() :
					((Number)queryDataUpper.getDataObject()).longValue()));
				Long lowerVal = (queryDataLower == null ? null : (queryDataLower instanceof RefexDynamicLongBI ? ((RefexDynamicLongBI)queryDataLower).getDataLong() :
					((Number)queryDataLower.getDataObject()).longValue()));
				bq.add(NumericRangeQuery.newLongRange(columnName, lowerVal, upperVal, queryDataLowerInclusive, queryDataUpperInclusive), Occur.SHOULD);
				if ((upperVal != null && upperVal <= Integer.MAX_VALUE && upperVal >= Integer.MIN_VALUE) 
						|| (lowerVal != null && lowerVal <= Integer.MAX_VALUE && lowerVal >= Integer.MIN_VALUE))
				{
					fitsInInt = true;
				}
			}
			
			if (fitsInInt || queryDataLower instanceof RefexDynamicIntegerBI || queryDataUpper instanceof RefexDynamicIntegerBI)
			{
				Integer upperVal = (queryDataUpper == null ? null : 
					(queryDataUpper instanceof RefexDynamicIntegerBI ? ((RefexDynamicIntegerBI)queryDataUpper).getDataInteger() :
						(fitsInInt && ((Number)queryDataUpper.getDataObject()).longValue() > Integer.MAX_VALUE ? Integer.MAX_VALUE :
							((Number)queryDataUpper.getDataObject()).intValue())));
				Integer lowerVal = (queryDataLower == null ? null : 
					(queryDataLower instanceof RefexDynamicIntegerBI ? ((RefexDynamicIntegerBI)queryDataLower).getDataInteger() :
						(fitsInInt && ((Number)queryDataLower.getDataObject()).longValue() < Integer.MIN_VALUE ? Integer.MIN_VALUE :
							((Number)queryDataLower.getDataObject()).intValue())));
				bq.add(NumericRangeQuery.newIntRange(columnName, lowerVal, upperVal, queryDataLowerInclusive, queryDataUpperInclusive), Occur.SHOULD);
			}
			if (bq.getClauses().length == 0)
			{
				throw new ParseException("Not a numeric data type - can't perform a range query");
			}
			else
			{
				BooleanQuery must = new BooleanQuery();
				must.add(bq, Occur.MUST);
				return must;
			}
		}
		catch (ClassCastException e)
		{
			throw new ParseException("One of the values is not a numeric data type - can't perform a range query");
		}
	}
	
	private abstract class QueryWrapperForColumnHandling
	{
		abstract Query buildQuery(String columnName) throws ParseException, IOException;
		
		protected Query buildColumnHandlingQuery(Integer[] searchColumns) throws ParseException, IOException
		{
			if (searchColumns == null || searchColumns.length == 0)
			{
				return buildQuery(COLUMN_FIELD_DATA);
			}
			else
			{
				BooleanQuery group = new BooleanQuery();
				for (int i : searchColumns)
				{
					group.add(buildQuery(COLUMN_FIELD_DATA + "_" + i), Occur.SHOULD);
				}
				return group;
			}
		}
	}
}
