package org.janusgraph.diskstorage.couchbase;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Storage Manager for Couchbase
 */
@PreInitializeConfigOptions
public class CouchbaseStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {

    private static final Logger logger = LoggerFactory.getLogger(CouchbaseStoreManager.class);

    private static final CouchbaseColumnConverter columnConverter = CouchbaseColumnConverter.INSTANCE;
    public static final ConfigNamespace COUCHBASE_NS =
        new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "couchbase", "Couchbase storage options");


    public static final int PORT_DEFAULT = 8091;  // Not used. Just for the parent constructor.

    public static final TimestampProviders PREFERRED_TIMESTAMPS = TimestampProviders.MILLI;

    // Immutable instance fields
    private final BucketWrapper bucket;

    // Mutable instance state
    private final ConcurrentMap<String, CouchbaseKeyColumnValueStore> openStores;


    private static final ConcurrentHashMap<CouchbaseStoreManager, Throwable> openManagers = new ConcurrentHashMap<>();


    public CouchbaseStoreManager(org.janusgraph.diskstorage.configuration.Configuration config) throws BackendException {
        super(config, PORT_DEFAULT);

        bucket = new BucketWrapper(config, hostnames, port, username, password);
        bucket.ensureBucketExists();

        if (logger.isTraceEnabled()) {
            openManagers.put(this, new Throwable("Manager Opened"));
            dumpOpenManagers();
        }

        openStores = new ConcurrentHashMap<>();
    }

    @Override
    public Deployment getDeployment() {
        return Deployment.REMOTE;
    }

    @Override
    public String toString() {
        return "couchbase[" + bucket.getBucketName() + "@" + super.toString() + "]";
    }

    public void dumpOpenManagers() {
        int estimatedSize = openManagers.size();
        logger.trace("---- Begin open Couchbase store manager list ({} managers) ----", estimatedSize);
        for (CouchbaseStoreManager m : openManagers.keySet()) {
            logger.trace("Manager {} opened at:", m, openManagers.get(m));
        }
        logger.trace("----   End open Couchbase store manager list ({} managers)  ----", estimatedSize);
    }

    @Override
    public void close() {
        openStores.clear();
        if (logger.isTraceEnabled())
            openManagers.remove(this);

        bucket.close();
    }

    @Override
    public StoreFeatures getFeatures() {

        Configuration c = GraphDatabaseConfiguration.buildGraphConfiguration();

        StandardStoreFeatures.Builder fb = new StandardStoreFeatures.Builder()
            .orderedScan(true).unorderedScan(true).batchMutation(true)
            .multiQuery(true).distributed(true).keyOrdered(true)
            .cellTTL(true).timestamps(true).preferredTimestamps(PREFERRED_TIMESTAMPS)
            .optimisticLocking(true).keyConsistent(c);

        try {
            fb.localKeyPartition(getDeployment() == Deployment.LOCAL);
        } catch (Exception e) {
            logger.warn("Unexpected exception during getDeployment()", e);
        }

        return fb.build();
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> batch, StoreTransaction txh)
        throws BackendException {
        final MaskedTimestamp commitTime = new MaskedTimestamp(txh);
        final List<CouchbaseDocumentMutation> documentMutations = convertToDocumentMutations(batch);

        try {
            Observable
                .from(documentMutations)
                .flatMap(docMutation -> {
                    final JsonDocument document = getMutatedDocument(docMutation);
                    final Map<String, CouchbaseColumn> columns = getMutatedColumns(docMutation, document);

                    if (!columns.isEmpty()) {
                        updateColumns(document, columns);
                        return bucket.asyncUpsert(document);
                    } else
                        return bucket.asyncRemove(document);
                })
                .last()
                .toBlocking()
                .single();
        } catch (CouchbaseException e) {
            throw new TemporaryBackendException(e);
        }

        sleepAfterWrite(txh, commitTime);
    }

    public void mutate(CouchbaseDocumentMutation docMutation, StoreTransaction txh) throws BackendException {
        final MaskedTimestamp commitTime = new MaskedTimestamp(txh);

        try {
            final JsonDocument document = getMutatedDocument(docMutation);
            final Map<String, CouchbaseColumn> columns = getMutatedColumns(docMutation, document);

            if (!columns.isEmpty()) {
                updateColumns(document, columns);
                bucket.upsert(document);
            } else
                bucket.remove(document);
        } catch (CouchbaseException e) {
            throw new TemporaryBackendException(e);
        }

        sleepAfterWrite(txh, commitTime);
    }

    private JsonDocument getMutatedDocument(CouchbaseDocumentMutation docMutation) {
        // we should get whole document to clean up expired columns otherwise we could mutate document's fragments
        JsonDocument document = bucket.get(docMutation.getDocumentId());

        if (document == null)
            document = JsonDocument.create(
                docMutation.getDocumentId(),
                JsonObject.create()
                    .put(CouchbaseColumn.TABLE, docMutation.getTable())
                    .put(CouchbaseColumn.COLUMNS, JsonArray.create())
            );

        return document;
    }

    private Map<String, CouchbaseColumn> getMutatedColumns(CouchbaseDocumentMutation docMutation,
                                                           JsonDocument document) {
        final long currentTimeMillis = currentTimeMillis();

        final Map<String, CouchbaseColumn> columns = getColumnsFromDocument(document, currentTimeMillis);
        final KCVMutation mutation = docMutation.getMutation();

        if (mutation.hasAdditions()) {
            for (Entry e : mutation.getAdditions()) {
                final int ttl = getTtl(e);
                final String key = columnConverter.toString(e.getColumn());
                columns.put(key, new CouchbaseColumn(key,
                    columnConverter.toString(e.getValue()), getExpire(currentTimeMillis, ttl), ttl));
            }
        }

        if (mutation.hasDeletions()) {
            for (StaticBuffer b : mutation.getDeletions())
                columns.remove(columnConverter.toString(b));
        }

        return columns;
    }

    private long getExpire(long writetime, int ttl) {
        return writetime + ttl * 1000L;
    }

    private int getTtl(Entry e) {
        final Integer ttl = (Integer) e.getMetaData().get(EntryMetaData.TTL);
        return null != ttl && ttl > 0 ? ttl : Integer.MAX_VALUE;
    }

    private void updateColumns(JsonDocument document, Map<String, CouchbaseColumn> columns) {
        final List<JsonObject> columnsList = columns.entrySet().stream().map(entry ->
            JsonObject.create()
                .put(CouchbaseColumn.KEY, entry.getKey())
                .put(CouchbaseColumn.VALUE, entry.getValue().getValue())
                .put(CouchbaseColumn.EXPIRE, entry.getValue().getExpire())
                .put(CouchbaseColumn.TTL, entry.getValue().getTtl())
        ).collect(Collectors.toList());

        document.content().put(CouchbaseColumn.COLUMNS, JsonArray.from(columnsList));
    }

    private Map<String, CouchbaseColumn> getColumnsFromDocument(JsonDocument document, long currentTimeMillis) {
        final Map<String, CouchbaseColumn> columns = new HashMap<>();
        final Iterator it = document.content().getArray(CouchbaseColumn.COLUMNS).iterator();

        while (it.hasNext()) {
            final JsonObject column = (JsonObject) it.next();
            final long expire = column.getLong(CouchbaseColumn.EXPIRE);

            if (expire > currentTimeMillis) {
                final String key = column.getString(CouchbaseColumn.KEY);
                columns.put(key, new CouchbaseColumn(key, column.getString(CouchbaseColumn.VALUE), expire,
                    column.getInt(CouchbaseColumn.TTL)));
            }
        }

        return columns;
    }

    private List<CouchbaseDocumentMutation> convertToDocumentMutations(Map<String, Map<StaticBuffer, KCVMutation>> batch) {
        final List<CouchbaseDocumentMutation> documentMutations = new ArrayList<>();

        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> batchEntry : batch.entrySet()) {
            final String table = batchEntry.getKey();
            Preconditions.checkArgument(openStores.containsKey(table), "Table cannot be found: " + table);

            final Map<StaticBuffer, KCVMutation> mutations = batchEntry.getValue();
            for (Map.Entry<StaticBuffer, KCVMutation> ent : mutations.entrySet()) {
                final KCVMutation mutation = ent.getValue();
                final String id = columnConverter.toString(ent.getKey());
                documentMutations.add(new CouchbaseDocumentMutation(table, id, mutation));
            }
        }

        return documentMutations;
    }

    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    @Override
    public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException {
        CouchbaseKeyColumnValueStore store = openStores.get(name);

        if (store == null) {
            final CouchbaseKeyColumnValueStore newStore = new CouchbaseKeyColumnValueStore(this,
                bucket.getBucketName(), name, bucket);

            store = openStores.putIfAbsent(name, newStore);

            if (store == null) {
                try {
                    bucket.ensureBucketExists();
                } catch (CouchbaseException e) {
                    throw new TemporaryBackendException(e);
                }
                store = newStore;
            }
        }

        return store;
    }

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig config) {
        return new CouchbaseTransaction(config);
    }

    @Override
    public String getName() {
        return bucket.getBucketName();
    }

    /**
     * Deletes the specified table with all its columns.
     */
    @Override
    public void clearStorage() throws BackendException {
        try {
            if (this.storageConfig.get(GraphDatabaseConfiguration.DROP_ON_CLEAR))
                bucket.dropBucket();
            else
                bucket.truncateBucket();
        } catch (CouchbaseException e) {
            throw new TemporaryBackendException(e);
        }
    }

    @Override
    public boolean exists() throws BackendException {
        try {
            return bucket.bucketExists();
        } catch (CouchbaseException e) {
            throw new TemporaryBackendException(e);
        }
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() {
        throw new UnsupportedOperationException();
    }


}
