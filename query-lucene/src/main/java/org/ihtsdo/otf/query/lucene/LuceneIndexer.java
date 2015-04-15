package org.ihtsdo.otf.query.lucene;

//~--- non-JDK imports --------------------------------------------------------
import static gov.vha.isaac.lookup.constants.Constants.SEARCH_ROOT_LOCATION_PROPERTY;
import gov.vha.isaac.ochre.api.sememe.SememeChronicle;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;

import org.ihtsdo.otf.tcc.api.blueprint.ComponentProperty;
import org.ihtsdo.otf.tcc.api.chronicle.ComponentChronicleBI;
import org.ihtsdo.otf.tcc.api.thread.NamedThreadFactory;
import org.ihtsdo.otf.tcc.model.cc.termstore.TermstoreLogger;
import org.ihtsdo.otf.tcc.model.index.service.IndexedGenerationCallable;
import org.ihtsdo.otf.tcc.model.index.service.IndexerBI;
import org.ihtsdo.otf.tcc.model.index.service.SearchResult;

//~--- JDK imports ------------------------------------------------------------
import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.index.TrackingIndexWriter;
import org.apache.lucene.search.ControlledRealTimeReopenThread;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherManager;

// See example for help with the Controlled Real-time indexing...
// http://stackoverflow.com/questions/17993960/lucene-4-4-0-new-controlledrealtimereopenthread-sample-usage?answertab=votes#tab-top

public abstract class LuceneIndexer implements IndexerBI {


    public static final String LUCENE_ROOT_LOCATION_PROPERTY
            = SEARCH_ROOT_LOCATION_PROPERTY;
    public static final String DEFAULT_LUCENE_FOLDER = "lucene";
    protected static final Logger logger
            = Logger.getLogger(LuceneIndexer.class.getName());
    public static final Version luceneVersion = Version.LUCENE_4_10_3;
    private static final UnindexedFuture unindexedFuture = new UnindexedFuture();
    private static final ThreadGroup threadGroup = new ThreadGroup("Lucene");
    public static File root;
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
    private boolean enabled = true;
    protected final ExecutorService luceneWriterService;
    protected ExecutorService luceneWriterFutureCheckerService;
    private final ControlledRealTimeReopenThread<IndexSearcher> reopenThread;
    private final TrackingIndexWriter trackingIndexWriter;
    private final ReferenceManager<IndexSearcher> searcherManager;
    private final String indexName;

    public LuceneIndexer(String indexName) throws IOException {
        this.indexName = indexName;
        luceneWriterService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                new NamedThreadFactory(threadGroup, indexName + " Lucene writer"));
        luceneWriterFutureCheckerService = Executors.newFixedThreadPool(1,
                new NamedThreadFactory(threadGroup, indexName + " Lucene future checker"));
        setupRoot();

        File indexDirectoryFile = new File(root.getPath() + "/" + indexName);
        System.out.println("Index: " + indexDirectoryFile);
        Directory indexDirectory = initDirectory(indexDirectoryFile);

        indexDirectory.clearLock("write.lock");

        IndexWriterConfig config = new IndexWriterConfig(luceneVersion, new StandardAnalyzer(luceneVersion));
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
        reopenThread = new ControlledRealTimeReopenThread<>(trackingIndexWriter,
                    searcherManager, 60.00, 0.1);
    
        this.startThread();

    }

    private static void setupRoot() {
        String rootLocation = System.getProperty(LUCENE_ROOT_LOCATION_PROPERTY);

        if (rootLocation != null) {
            root = new File(rootLocation, DEFAULT_LUCENE_FOLDER);
        } else {
            rootLocation = System.getProperty("org.ihtsdo.otf.tcc.datastore.bdb-location");

            if (rootLocation != null) {
                root = new File(rootLocation, DEFAULT_LUCENE_FOLDER);
            } else {
                root = new File(DEFAULT_LUCENE_FOLDER);
            }
        }
    }

    private void startThread() {
        reopenThread.setName("Lucene " + indexName + " Reopen Thread");
        reopenThread.setPriority(Math.min(Thread.currentThread().getPriority() + 2, Thread.MAX_PRIORITY));
        reopenThread.setDaemon(true);
        reopenThread.start();
    }

    @Override
    public String getIndexerName() {
        return indexName;
    }

    protected abstract boolean indexChronicle(ComponentChronicleBI chronicle);

    private static Directory initDirectory(File luceneDirFile)
            throws IOException, CorruptIndexException, LockObtainFailedException {
        if (luceneDirFile.exists()) {
            return new SimpleFSDirectory(luceneDirFile);
        } else {
            luceneDirFile.mkdirs();

            return new SimpleFSDirectory(luceneDirFile);
        }
    }

    /**
     * Query index with no specified target generation of the index.
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
    public final List<SearchResult> query(String query, ComponentProperty field, int sizeLimit)
            throws IOException {
            return query(query, field, sizeLimit, Long.MIN_VALUE);
    }

    /**
     *
     * @param query The query to apply.
     * @param field The component field to be queried.
     * @param sizeLimit The maximum size of the result list.
     * @param targetGeneration target generation that must be included in the
     * search or Long.MIN_VALUE if there is no need to wait for a target
     * generation.
     * @return a List of <code>SearchResult</codes> that contins the nid of the
     * component that matched, and the score of that match relative to other
     * matches.
     * @throws IOException
     */
    @Override
    public final List<SearchResult> query(String query, ComponentProperty field, int sizeLimit, long targetGeneration)
            throws IOException {
        try {
            List<SearchResult> result;

            switch (field) {
                case STRING_EXTENSION_1:
                case DESCRIPTION_TEXT:
                    Query q = new QueryParser(LuceneIndexer.luceneVersion, field.name(),
                            new StandardAnalyzer(LuceneIndexer.luceneVersion)).parse(query);

                    result = search(q, sizeLimit, targetGeneration);

                    if (result.size() > 0) {
                        if (TermstoreLogger.logger.isLoggable(Level.FINE)) {
                            TermstoreLogger.logger.log(Level.FINE, "StandardAnalyzer query returned {0} hits",
                                    result.size());
                        }
                    } else {
                        if (TermstoreLogger.logger.isLoggable(Level.FINE)) {
                            TermstoreLogger.logger.fine("StandardAnalyzer query returned no results. "
                                    + "Now trying WhitespaceAnalyzer query");
                            q = new QueryParser(LuceneIndexer.luceneVersion, field.name(),
                                    new WhitespaceAnalyzer(LuceneIndexer.luceneVersion)).parse(query);
                        }

                        result = search(q, sizeLimit, targetGeneration);
                    }

                    break;

                default:
                    throw new IOException("Can't handle: " + field.name());
            }

            return result;
        } catch (ParseException | IOException | NumberFormatException e) {
            throw new IOException(e);
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

    protected abstract void addFields(ComponentChronicleBI chronicle, Document doc);

    @Override
    public final Future<Long> index(ComponentChronicleBI chronicle) {
        if (!enabled) {
            return null;
        }

        if (indexChronicle(chronicle)) {
            Future<Long> future = luceneWriterService.submit(new AddDocument(chronicle));

            luceneWriterFutureCheckerService.execute(new FutureChecker(future));

            return future;
        }

        return unindexedFuture;
    }

    private List<SearchResult> search(Query q, int sizeLimit, long targetGeneration) throws IOException {
        if (targetGeneration != Long.MIN_VALUE) {
            try {
                reopenThread.waitForGeneration(targetGeneration);
            } catch (InterruptedException ex) {
                Logger.getLogger(LuceneIndexer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        IndexSearcher searcher = searcherManager.acquire();

        try {
            TopDocs topDocs = searcher.search(q, sizeLimit);
            List<SearchResult> results = new ArrayList<>(topDocs.totalHits);

            for (ScoreDoc hit : topDocs.scoreDocs) {
                if (TermstoreLogger.logger.isLoggable(Level.FINE)) {
                    TermstoreLogger.logger.log(Level.FINE, "Hit: {0} Score: {1}", new Object[]{hit.doc, hit.score});
                }

                Document doc = searcher.doc(hit.doc);

                results.add(
                        new SearchResult(
                                doc.getField(ComponentProperty.COMPONENT_ID.name()).numericValue().intValue(), hit.score));
            }

            return results;
        } finally {
            searcherManager.release(searcher);
        }
    }

    /**
     *
     * @param nid for the component that the caller wished to wait until it's
     * document is added to the index.
     * @return a <code>Callable&lt;Long&gt;</code> object that will block until
     * this indexer has added the document to the index. The <code>call()</code>
     * method on the object will return the index generation that contains the
     * document, which can be used in search calls to make sure the generation
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
        this.enabled = enabled;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }
    
    @Override
    public File getIndexerFolder() {
        return root;
    }


    private class AddDocument implements Callable<Long> {

        ComponentChronicleBI chronicle;

        public AddDocument(ComponentChronicleBI chronicle) {
            this.chronicle = chronicle;
        }

        @Override
        public Long call() throws Exception {
            IndexedGenerationCallable latch = componentNidLatch.remove(chronicle.getNid());
            Document doc = new Document();

            doc.add(new IntField(ComponentProperty.COMPONENT_ID.name(), chronicle.getNid(),
                    LuceneIndexer.indexedComponentNidType));
            addFields(chronicle, doc);

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

            if (latch != null) {
                latch.setIndexGeneration(indexGeneration);
            }

            return indexGeneration;
        }
    }

    /**
     * Class to ensure that any exceptions associated with indexingFutures are
     * properly logged.
     */
    private static class FutureChecker implements Runnable {

        Future future;

        public FutureChecker(Future future) {
            this.future = future;
        }

        @Override
        public void run() {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException ex) {
                Logger.getLogger(LuceneIndexer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
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

    @Override
    public Future<Long> index(SememeChronicle chronicle) {
        if (!enabled) {
            return null;
        }

        if (indexSememeChronicle(chronicle)) {
            Future<Long> future = luceneWriterService.submit(new AddSememeDocument(chronicle));

            luceneWriterFutureCheckerService.execute(new FutureChecker(future));

            return future;
        }

        return unindexedFuture;
    }
    
    protected abstract boolean indexSememeChronicle(SememeChronicle chronicle);
    
    protected abstract void addFields(SememeChronicle chronicle, Document doc);

    private class AddSememeDocument implements Callable<Long> {

        SememeChronicle chronicle;

        public AddSememeDocument(SememeChronicle chronicle) {
            this.chronicle = chronicle;
        }

        @Override
        public Long call() throws Exception {
            IndexedGenerationCallable latch = componentNidLatch.remove(chronicle.getNid());
            Document doc = new Document();
            addFields(chronicle, doc);

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

            if (latch != null) {
                latch.setIndexGeneration(indexGeneration);
            }

            return indexGeneration;
        }
    }

    
}
