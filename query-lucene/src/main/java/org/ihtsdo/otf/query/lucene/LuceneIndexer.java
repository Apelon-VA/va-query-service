package org.ihtsdo.otf.query.lucene;

import gov.vha.isaac.ochre.api.ConfigurationService;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.SystemStatusService;
import gov.vha.isaac.ochre.api.sememe.SememeChronicle;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;
import org.ihtsdo.otf.tcc.api.blueprint.ComponentProperty;
import org.ihtsdo.otf.tcc.api.chronicle.ComponentChronicleBI;
import org.ihtsdo.otf.tcc.api.thread.NamedThreadFactory;
import org.ihtsdo.otf.tcc.model.cc.termstore.TermstoreLogger;
import org.ihtsdo.otf.tcc.model.index.service.IndexedGenerationCallable;
import org.ihtsdo.otf.tcc.model.index.service.IndexerBI;
import org.ihtsdo.otf.tcc.model.index.service.SearchResult;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.index.TrackingIndexWriter;
import org.apache.lucene.search.ControlledRealTimeReopenThread;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherManager;

// See example for help with the Controlled Real-time indexing...
// http://stackoverflow.com/questions/17993960/lucene-4-4-0-new-controlledrealtimereopenthread-sample-usage?answertab=votes#tab-top

public abstract class LuceneIndexer implements IndexerBI {

    public static final String DEFAULT_LUCENE_FOLDER = "lucene";
    private static final Logger logger = Logger.getLogger(LuceneIndexer.class.getName());
    public static final Version luceneVersion = Version.LUCENE_4_10_3;
    private static final UnindexedFuture unindexedFuture = new UnindexedFuture();
    private static final ThreadGroup threadGroup = new ThreadGroup("Lucene");
    
    private static AtomicReference<File> luceneRootFolder_ = new AtomicReference<>();
    private File indexFolder_ = null;
    
    protected static final FieldType indexedComponentNidType;
    protected static final FieldType referencedComponentNidType;

    static {
        indexedComponentNidType = new FieldType();
        indexedComponentNidType.setNumericType(FieldType.NumericType.INT);
        indexedComponentNidType.setIndexed(false);
        indexedComponentNidType.setStored(true);
        indexedComponentNidType.setTokenized(false);
        indexedComponentNidType.freeze();
        referencedComponentNidType = new FieldType();
        referencedComponentNidType.setNumericType(FieldType.NumericType.INT);
        referencedComponentNidType.setIndexed(true);
        referencedComponentNidType.setStored(false);
        referencedComponentNidType.setTokenized(false);
        referencedComponentNidType.freeze();
    }

    private final ConcurrentHashMap<Integer, IndexedGenerationCallable> componentNidLatch = new ConcurrentHashMap<>();
    private boolean enabled_ = true;
    protected final ExecutorService luceneWriterService;
    protected ExecutorService luceneWriterFutureCheckerService;
    private final ControlledRealTimeReopenThread<IndexSearcher> reopenThread;
    private final TrackingIndexWriter trackingIndexWriter;
    private final ReferenceManager<IndexSearcher> searcherManager;
    private final String indexName_;

    protected LuceneIndexer(String indexName) throws IOException {
        try {
            indexName_ = indexName;
            luceneWriterService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                    new NamedThreadFactory(threadGroup, indexName + " Lucene writer"));
            luceneWriterFutureCheckerService = Executors.newFixedThreadPool(1,
                    new NamedThreadFactory(threadGroup, indexName + " Lucene future checker"));
            
            Path searchFolder = LookupService.getService(ConfigurationService.class).getSearchFolderPath();
            
            if (luceneRootFolder_.compareAndSet(null, new File(searchFolder.toFile(), DEFAULT_LUCENE_FOLDER))) {
                luceneRootFolder_.get().mkdirs();
            }
            
            indexFolder_ = new File(luceneRootFolder_.get(), indexName);
            indexFolder_.mkdirs();

            logger.info("Index: " + indexFolder_.getAbsolutePath());
            Directory indexDirectory = new SimpleFSDirectory(indexFolder_); 

            indexDirectory.clearLock("write.lock");

            IndexWriterConfig config = new IndexWriterConfig(luceneVersion, new PerFieldAnalyzer());
            config.setRAMBufferSizeMB(256);
            MergePolicy mergePolicy = new LogByteSizeMergePolicy();

            config.setMergePolicy(mergePolicy);
            config.setSimilarity(new ShortTextSimilarity());

            IndexWriter indexWriter = new IndexWriter(indexDirectory, config);

            trackingIndexWriter = new TrackingIndexWriter(indexWriter);

            boolean applyAllDeletes = false;

            searcherManager = new SearcherManager(indexWriter, applyAllDeletes, null);
            // [3]: Create the ControlledRealTimeReopenThread that reopens the index periodically taking into 
            //      account the changes made to the index and tracked by the TrackingIndexWriter instance
            //      The index is refreshed every 60sc when nobody is waiting 
            //      and every 100 millis whenever is someone waiting (see search method)
            //      (see http://lucene.apache.org/core/4_3_0/core/org/apache/lucene/search/NRTManagerReopenThread.html)
            reopenThread = new ControlledRealTimeReopenThread<>(trackingIndexWriter, searcherManager, 60.00, 0.1);
   
            this.startThread();
        }
        catch (Exception e) {
            LookupService.getService(SystemStatusService.class).notifyServiceConfigurationFailure(indexName, e);
            throw e;
        }
    }

    private void startThread() {
        reopenThread.setName("Lucene " + indexName_ + " Reopen Thread");
        reopenThread.setPriority(Math.min(Thread.currentThread().getPriority() + 2, Thread.MAX_PRIORITY));
        reopenThread.setDaemon(true);
        reopenThread.start();
    }

    @Override
    public String getIndexerName() {
        return indexName_;
    }

    /**
     * Query index with no specified target generation of the index.
     * 
     * Calls {@link #query(String, ComponentProperty, int, long)} with the targetGeneration 
     * field set to Long.MIN_VALUE
     *
     * @param query The query to apply.
     * @param field The component field to be queried.
     * @param sizeLimit The maximum size of the result list.
     * @return a List of <code>SearchResult</codes> that contins the nid of the
     * component that matched, and the score of that match relative to other
     * matches.
     * @throws IOException
     */
    @Override
    public final List<SearchResult> query(String query, ComponentProperty field, int sizeLimit) throws IOException {
        return query(query, field, sizeLimit, Long.MIN_VALUE);
    }
    
    /**
    *
    *Calls {@link #query(String, boolean, ComponentProperty, int, long)} with the prefixSearch field set to false.
    *
    * @param query The query to apply.
    * @param field The component field to be queried.
    * @param sizeLimit The maximum size of the result list.
    * @param targetGeneration target generation that must be included in the search or Long.MIN_VALUE if there is no 
    * need to wait for a target generation.  Long.MAX_VALUE can be passed in to force this query to wait until any 
    * in-progress indexing operations are completed - and then use the latest index.
    * @return a List of <code>SearchResult</codes> that contins the nid of the component that matched, and the score of 
    * that match relative to other matches.
    * @throws IOException
    */
   @Override
    public final List<SearchResult> query(String query, ComponentProperty field, int sizeLimit, long targetGeneration) throws IOException {
       return query(query, false, field, sizeLimit, targetGeneration);
   }

    /**
     * A generic query API that handles most common cases.  The cases handled for various component property types
     * are detailed below.
     * 
     * NOTE - subclasses of LuceneIndexer may have other query(...) methods that allow for more specific and or complex
     * queries.  Specifically both {@link LuceneDynamicRefexIndexer} and {@link LuceneDescriptionIndexer} have their own 
     * query(...) methods which allow for more advanced queries.
     *
     * @param query The query to apply.
     * @param field The component field to be queried.
     * @param sizeLimit The maximum size of the result list.
     * @param targetGeneration target generation that must be included in the search or Long.MIN_VALUE if there is no need 
     * to wait for a target generation.  Long.MAX_VALUE can be passed in to force this query to wait until any in progress 
     * indexing operations are completed - and then use the latest index.
     * @param prefixSearch if true, utilize a search algorithm that is optimized for prefix searching, such as the searching 
     * that would be done to implement a type-ahead style search.  This is currently only applicable to 
     * {@link ComponentProperty#DESCRIPTION_TEXT} cases - is ignored for all other field types.  Does not use the Lucene 
     * Query parser.  Every term (or token) that is part of the query string will be required to be found in the result.
     * 
     * Note, it is useful to NOT trim the text of the query before it is sent in - if the last word of the query has a 
     * space character following it, that word will be required as a complete term.  If the last word of the query does not 
     * have a space character following it, that word will be required as a prefix match only.
     * 
     * For example:
     * The query "family test" will return results that contain 'Family Testudinidae'
     * The query "family test " will not match on  'Testudinidae', so that will be excluded.
     * 
     * At the moment, the only supported ComponentProperty types for a search are:
     * 
     * - {@link ComponentProperty#STRING_EXTENSION_1} - currently, has identical behavior to {@link ComponentProperty#DESCRIPTION_TEXT}
     * 
     * - {@link ComponentProperty#DESCRIPTION_TEXT} - this is the property value you pass in to search all indexed description types.
     *     
     * - {@link ComponentProperty#ASSEMBLAGE_ID} - This is the property value you pass in to search for all concepts which have references 
     *   to a particular Dynamic Refex Assemblage - and that particular Dynamic Refex Assemblage is defined as an annotation style refex.  
     *
     * @return a List of {@link SearchResult} that contains the nid of the component that matched, and the score of that match relative 
     * to other matches.
     * @throws IOException if anything goes wrong during query processing
     */
    public final List<SearchResult> query(String query, boolean prefixSearch, ComponentProperty field, int sizeLimit, Long targetGeneration)
            throws IOException {

        switch (field) {
            case STRING_EXTENSION_1:
            case DESCRIPTION_TEXT:
                try
                {
                    return search(buildTokenizedStringQuery(query, field.name(), prefixSearch), sizeLimit, targetGeneration);
                }
                catch (ParseException e)
                {
                    throw new IOException(e);
                }

            case ASSEMBLAGE_ID:
                Query termQuery = new TermQuery(new Term(LuceneDynamicRefexIndexer.COLUMN_FIELD_ASSEMBLAGE, query));
                return search(termQuery, sizeLimit, targetGeneration);

            default:
                throw new IOException("Can't handle: " + field.name());
        }
    }

    @Override
    public final void clearIndex() {
        try {
            trackingIndexWriter.deleteAll();
        } catch (IOException ex) {
            Logger.getLogger(LuceneRefexIndexer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void forceMerge() {
        try {
            trackingIndexWriter.getIndexWriter().forceMerge(1);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public final void closeWriter() {
        try {
            reopenThread.close();
            luceneWriterService.shutdown();
            luceneWriterService.awaitTermination(15, TimeUnit.MINUTES);
            luceneWriterFutureCheckerService.shutdown();
            luceneWriterFutureCheckerService.awaitTermination(15, TimeUnit.MINUTES);
            trackingIndexWriter.getIndexWriter().close();
        } catch (IOException ex) {
            Logger.getLogger(LuceneRefexIndexer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(LuceneIndexer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public final void commitWriter() {
        try {
            trackingIndexWriter.getIndexWriter().commit();
        } catch (IOException ex) {
            Logger.getLogger(LuceneRefexIndexer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Subclasses may call this method with much more specific queries than this generic class is capable of constructing.
     */
    protected final List<SearchResult> search(Query q, int sizeLimit, Long targetGeneration) throws IOException {
        if (targetGeneration != null && targetGeneration != Long.MIN_VALUE) {
            if (targetGeneration == Long.MAX_VALUE)
            {
                searcherManager.maybeRefreshBlocking();
            }
            else
            {
                try
                {
                    reopenThread.waitForGeneration(targetGeneration);
                }
                catch (InterruptedException e)
                {
                    throw new IOException(e);
                }
            }
        }
        
        IndexSearcher searcher = searcherManager.acquire();

        try 
        {
            if (TermstoreLogger.logger.isLoggable(Level.FINE)) 
            {
                TermstoreLogger.logger.log(Level.FINE, "Running query: " + q.toString());
            }
            
            //Since the index carries some duplicates by design, which we will remove - get a few extra results up front.
            //so we are more likely to come up with the requested number of results
            long limitWithExtras = sizeLimit + (long)((double)sizeLimit * 0.25d);
            
            int adjustedLimit = (limitWithExtras > Integer.MAX_VALUE ? sizeLimit : (int)limitWithExtras);
            
            TopDocs topDocs = searcher.search(q, adjustedLimit);
            List<SearchResult> results = new ArrayList<>(topDocs.totalHits);
            HashSet<Integer> includedComponentIDs = new HashSet<>();

            for (ScoreDoc hit : topDocs.scoreDocs) 
            {
                if (TermstoreLogger.logger.isLoggable(Level.FINEST)) 
                {
                    TermstoreLogger.logger.log(Level.FINEST, "Hit: {0} Score: {1}", new Object[]{hit.doc, hit.score});
                }

                Document doc = searcher.doc(hit.doc);
                int componentId = doc.getField(ComponentProperty.COMPONENT_ID.name()).numericValue().intValue();
                if (includedComponentIDs.contains(componentId))
                {
                    continue;
                }
                else
                {
                    includedComponentIDs.add(componentId);
                    results.add(new SearchResult(componentId, hit.score));
                    if (results.size() == sizeLimit)
                    {
                        break;
                    }
                }
            }
            if (TermstoreLogger.logger.isLoggable(Level.FINE)) 
            {
                TermstoreLogger.logger.log(Level.FINE, "Returning " + results.size() + " results from query");
            }
            return results;
        } finally {
            searcherManager.release(searcher);
        }
    }

    /**
     *
     * @param nid for the component that the caller wished to wait until it's document is added to the index.
     * @return a {@link IndexedGenerationCallable} object that will block until this indexer has added the 
     * document to the index. The {@link IndexedGenerationCallable#call()} method on the object will return the 
     * index generation that contains the document, which can be used in search calls to make sure the generation
     * is available to the searcher.
     */
    @Override
    public IndexedGenerationCallable getIndexedGenerationCallable(int nid) {
        IndexedGenerationCallable indexedLatch = new IndexedGenerationCallable();
        IndexedGenerationCallable existingIndexedLatch = componentNidLatch.putIfAbsent(nid, indexedLatch);

        if (existingIndexedLatch != null) {
            return existingIndexedLatch;
        }

        return indexedLatch;
    }

    @Override
    public void setEnabled(boolean enabled) {
        enabled_ = enabled;
    }

    @Override
    public boolean isEnabled() {
        return enabled_;
    }
    
    @Override
    public File getIndexerFolder() {
        return indexFolder_;
    }

    protected void releaseLatch(int latchNid, long indexGeneration)
    {
        IndexedGenerationCallable latch = componentNidLatch.remove(latchNid);

        if (latch != null) {
            latch.setIndexGeneration(indexGeneration);
        }
    }

    /**
     * Create a query that will match on the specified text using either the WhitespaceAnalyzer or the StandardAnalyzer.
     * Uses the Lucene Query Parser if prefixSearch is false, otherwise, uses a custom prefix algorithm.  
     * See {@link LuceneIndexer#query(String, boolean, ComponentProperty, int, Long)} for details on the prefix search algorithm. 
     */
    protected Query buildTokenizedStringQuery(String query, String field, boolean prefixSearch) throws IOException, ParseException
    {
        BooleanQuery bq = new BooleanQuery();
        
        if (prefixSearch) 
        {
            bq.add(buildPrefixQuery(query,field, new PerFieldAnalyzer()), Occur.SHOULD);
            bq.add(buildPrefixQuery(query,field + PerFieldAnalyzer.WHITE_SPACE_FIELD_MARKER, new PerFieldAnalyzer()), Occur.SHOULD);
        }
        else {
            QueryParser qp1 = new QueryParser(field, new PerFieldAnalyzer());
            qp1.setAllowLeadingWildcard(true);
            bq.add(qp1.parse(query), Occur.SHOULD);
            QueryParser qp2 = new QueryParser(field + PerFieldAnalyzer.WHITE_SPACE_FIELD_MARKER, new PerFieldAnalyzer());
            qp2.setAllowLeadingWildcard(true);
            bq.add(qp2.parse(query), Occur.SHOULD);
        }
        BooleanQuery wrap = new BooleanQuery();
        wrap.add(bq, Occur.MUST);
        return wrap;
    }
    
    protected Query buildPrefixQuery(String searchString, String field, Analyzer analyzer) throws IOException
    {
        StringReader textReader = new StringReader(searchString);
        TokenStream tokenStream = analyzer.tokenStream(field, textReader);
        tokenStream.reset();
        List<String> terms = new ArrayList<>();
        CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
        
        while (tokenStream.incrementToken())
        {
            terms.add(charTermAttribute.toString());
        }
        textReader.close();
        tokenStream.close();
        analyzer.close();
        
        BooleanQuery bq = new BooleanQuery();
        if (terms.size() > 0 && !searchString.endsWith(" "))
        {
            String last = terms.remove(terms.size() - 1);
            bq.add(new PrefixQuery((new Term(field, last))), Occur.MUST);
        }
        for (String s : terms)
        {
            bq.add(new TermQuery(new Term(field, s)), Occur.MUST);
        }
        
        return bq;
    }
    
    @Override
    public final Future<Long> index(ComponentChronicleBI<?> chronicle) {
        return index((() -> new AddDocument(chronicle)), (() -> indexChronicle(chronicle)), chronicle.getNid());
    }
    
    @Override
    public Future<Long> index(SememeChronicle<?> chronicle) {
        return index((() -> new AddDocument(chronicle)), (() -> indexSememeChronicle(chronicle)), chronicle.getNid());
    }

    private final Future<Long> index(Supplier<AddDocument> documentSupplier, BooleanSupplier indexChronicle, int chronicleNid) {
        if (!enabled_) {
            releaseLatch(chronicleNid, Long.MIN_VALUE);
            return null;
        }

        if (indexChronicle.getAsBoolean()) {
            Future<Long> future = luceneWriterService.submit(documentSupplier.get());

            luceneWriterFutureCheckerService.execute(new FutureChecker(future));

            return future;
        }
        else
        {
            releaseLatch(chronicleNid, Long.MIN_VALUE);
        }

        return unindexedFuture;
    }
    
    private static class UnindexedFuture implements Future<Long> {

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Long get() throws InterruptedException, ExecutionException {
            return Long.MIN_VALUE;
        }

        @Override
        public Long get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return Long.MIN_VALUE;
        }
    }
    
    /**
     * Class to ensure that any exceptions associated with indexingFutures are properly logged.
     */
    private static class FutureChecker implements Runnable {

        Future<Long> future_;

        public FutureChecker(Future<Long> future) {
            future_ = future;
        }

        @Override
        public void run() {
            try {
                future_.get();
            } catch (InterruptedException | ExecutionException ex) {
                Logger.getLogger(LuceneIndexer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    private class AddDocument implements Callable<Long> {

        ComponentChronicleBI<?> chronicle_ = null;
        SememeChronicle<?> sememeChronicle_ = null;

        public AddDocument(ComponentChronicleBI<?> chronicle) {
            chronicle_ = chronicle;
        }
        
        public AddDocument(SememeChronicle<?> chronicle) {
            sememeChronicle_ = chronicle;
        }
        
        public int getNid() {
            return chronicle_ == null ? sememeChronicle_.getNid() : chronicle_.getNid();
        }

        @Override
        public Long call() throws Exception {
            Document doc = new Document();
            
            if (chronicle_ == null)
            {
                //TODO dan hacking - Keith question - for some reason, Keith isn't putting a field in sememe chronicles with the id???
                //See other notes on issue in LuceneRefexIndexer
                addFields(sememeChronicle_, doc);
            }
            else
            {
                doc.add(new IntField(ComponentProperty.COMPONENT_ID.name(), getNid(), LuceneIndexer.indexedComponentNidType));
                addFields(chronicle_, doc);
            }

            // Note that the addDocument operation could cause duplicate documents to be
            // added to the index if a new luceneVersion is added after initial index
            // creation. It does this to avoid the performance penalty of
            // finding and deleting documents prior to inserting a new one.
            //
            // At this point, the number of duplicates should be
            // small, and we are willing to accept a small number of duplicates
            // because the new versions are additive (we don't allow deletion of content)
            // so the search results will be the same. Duplicates can be removed
            // by regenerating the index.
            long indexGeneration = trackingIndexWriter.addDocument(doc);

            releaseLatch(getNid(), indexGeneration);

            return indexGeneration;
        }
    }

    protected abstract boolean indexChronicle(ComponentChronicleBI<?> chronicle);
    protected abstract boolean indexSememeChronicle(SememeChronicle<?> chronicle);
    protected abstract void addFields(SememeChronicle<?> chronicle, Document doc);
    protected abstract void addFields(ComponentChronicleBI<?> chronicle, Document doc);
}
