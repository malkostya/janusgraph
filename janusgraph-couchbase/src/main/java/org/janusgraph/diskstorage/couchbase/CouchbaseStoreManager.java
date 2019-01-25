// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
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

import static com.couchbase.client.java.query.Delete.deleteFrom;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;

/**
 * Storage Manager for Couchbase
 */
@PreInitializeConfigOptions
public class CouchbaseStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {

    private static final Logger logger = LoggerFactory.getLogger(CouchbaseStoreManager.class);

    public static final ConfigNamespace COUCHBASE_NS =
        new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "couchbase", "Couchbase storage options");

    public static final ConfigOption<String> COUCHBASE_BUCKET =
        new ConfigOption<>(COUCHBASE_NS, "bucket",
            "The name of the bucket JanusGraph will use. JanusGraph is not able to create a new bucket but" +
                " should use predefined one." +
                " If this configuration option is not provided but graph.graphname is, the bucket will be set" +
                " to that value.",
            ConfigOption.Type.LOCAL, "janusgraph");

    public static final int PORT_DEFAULT = 2181;  // Not used. Just for the parent constructor.

    public static final TimestampProviders PREFERRED_TIMESTAMPS = TimestampProviders.MILLI;

    // Immutable instance fields
    private final String bucketName;
    private final Cluster cluster;
    private final Bucket bucket;

    // Mutable instance state
    private final ConcurrentMap<String, CouchbaseKeyColumnValueStore> openStores;


    private static final ConcurrentHashMap<CouchbaseStoreManager, Throwable> openManagers = new ConcurrentHashMap<>();


    public CouchbaseStoreManager(org.janusgraph.diskstorage.configuration.Configuration config) throws BackendException {
        super(config, PORT_DEFAULT);

        this.bucketName = determineTableName(config);

        cluster = CouchbaseCluster.create(hostnames);
        bucket = cluster.openBucket(bucketName);

//
//        shortCfNameMap = createShortCfMap(config);
//
//        Preconditions.checkArgument(null != shortCfNameMap);
//        Collection<String> shorts = shortCfNameMap.values();
//        Preconditions.checkArgument(Sets.newHashSet(shorts).size() == shorts.size());
//
//        this.compression = config.get(COMPRESSION);
//        this.regionCount = config.has(REGION_COUNT) ? config.get(REGION_COUNT) : -1;
//        this.regionsPerServer = config.has(REGIONS_PER_SERVER) ? config.get(REGIONS_PER_SERVER) : -1;
//        this.skipSchemaCheck = config.get(SKIP_SCHEMA_CHECK);
//        final String compatClass = config.has(COMPAT_CLASS) ? config.get(COMPAT_CLASS) : null;
//        this.compat = HBaseCompatLoader.getCompat(compatClass);
//
//        /*
//         * Specifying both region count options is permitted but may be
//         * indicative of a misunderstanding, so issue a warning.
//         */
//        if (config.has(REGIONS_PER_SERVER) && config.has(REGION_COUNT)) {
//            logger.warn("Both {} and {} are set in JanusGraph's configuration, but "
//                      + "the former takes precedence and the latter will be ignored.",
//                        REGION_COUNT, REGIONS_PER_SERVER);
//        }
//
//        /* This static factory calls HBaseConfiguration.addHbaseResources(),
//         * which in turn applies the contents of hbase-default.xml and then
//         * applies the contents of hbase-site.xml.
//         */
//        hconf = HBaseConfiguration.create();
//
//        // Copy a subset of our commons config into a Hadoop config
//        int keysLoaded=0;
//        Map<String,Object> configSub = config.getSubset(HBASE_CONFIGURATION_NAMESPACE);
//        for (Map.Entry<String,Object> entry : configSub.entrySet()) {
//            logger.info("HBase configuration: setting {}={}", entry.getKey(), entry.getValue());
//            if (entry.getValue()==null) continue;
//            hconf.set(entry.getKey(), entry.getValue().toString());
//            keysLoaded++;
//        }
//
//        logger.debug("HBase configuration: set a total of {} configuration values", keysLoaded);
//
//        // Special case for STORAGE_HOSTS
//        if (config.has(GraphDatabaseConfiguration.STORAGE_HOSTS)) {
//            String zkQuorumKey = "hbase.zookeeper.quorum";
//            String csHostList = Joiner.on(",").join(config.get(GraphDatabaseConfiguration.STORAGE_HOSTS));
//            hconf.set(zkQuorumKey, csHostList);
//            logger.info("Copied host list from {} to {}: {}", GraphDatabaseConfiguration.STORAGE_HOSTS, zkQuorumKey, csHostList);
//        }
//
//        // Special case for STORAGE_PORT
//        if (config.has(GraphDatabaseConfiguration.STORAGE_PORT)) {
//            String zkPortKey = "hbase.zookeeper.property.clientPort";
//            Integer zkPort = config.get(GraphDatabaseConfiguration.STORAGE_PORT);
//            hconf.set(zkPortKey, zkPort.toString());
//            logger.info("Copied Zookeeper Port from {} to {}: {}", GraphDatabaseConfiguration.STORAGE_PORT, zkPortKey, zkPort);
//        }
//
//        this.shortCfNames = config.get(SHORT_CF_NAMES);
//
//        try {
//            //this.cnx = HConnectionManager.createConnection(hconf);
//            this.cnx = compat.createConnection(hconf);
//        } catch (IOException e) {
//            throw new PermanentBackendException(e);
//        }
//
        if (logger.isTraceEnabled()) {
            openManagers.put(this, new Throwable("Manager Opened"));
            dumpOpenManagers();
        }
//
//        logger.debug("Dumping HBase config key=value pairs");
//        for (Map.Entry<String, String> entry : hconf) {
//            logger.debug("[HBaseConfig] " + entry.getKey() + "=" + entry.getValue());
//        }
//        logger.debug("End of HBase config key=value pairs");

        openStores = new ConcurrentHashMap<>();
    }

    @Override
    public Deployment getDeployment() {
        return Deployment.REMOTE;
    }

    @Override
    public String toString() {
        return "couchbase[" + bucketName + "@" + super.toString() + "]";
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

        try {
            if (bucket != null)
                bucket.close();
        } catch (Exception e) {
            logger.warn("Failed closing bucket " + bucketName, e);
        }

        try {
            if (cluster != null)
                cluster.disconnect();
        } catch (Exception e) {
            logger.warn("Failed disconnecting cluster", e);
        }
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
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> batch, StoreTransaction txh) throws BackendException {
        final MaskedTimestamp commitTime = new MaskedTimestamp(txh);
        final List<CouchbaseDocumentMutation> documentMutations = convertToDocumentMutations(batch);

        Observable
            .from(documentMutations)
            .flatMap(docMutation -> {
                final long currentTimeMillis = currentTimeMillis();
                // we should get whole document to clean up expired columns otherwise we could mutate document's fragments
                JsonDocument document = bucket.get(docMutation.getDocumentId()); // TODO add getAndLock option to enforce consistency

                if (document == null)
                    document = JsonDocument.create(
                        docMutation.getDocumentId(),
                        JsonObject.create()
                            .put(CouchbaseColumn.TABLE, docMutation.getTable())
                            .put(CouchbaseColumn.COLUMNS, JsonArray.create())
                    );

                Map<String, CouchbaseColumn> columns = getColumnsFromDocument(document, currentTimeMillis);
                KCVMutation mutation = docMutation.getMutation();

                if (mutation.hasAdditions()) {
                    for (Entry e : mutation.getAdditions()) {
                        Integer ttl = (Integer) e.getMetaData().get(EntryMetaData.TTL);

                        columns.put(e.getColumnAs(CouchbaseColumnConverter.INSTANCE), new CouchbaseColumn(
                            e.getValueAs(CouchbaseColumnConverter.INSTANCE), currentTimeMillis,
                            null != ttl && ttl > 0 ? ttl : 0));
                    }
                }

                if (mutation.hasDeletions()) {
                    for (StaticBuffer b : mutation.getDeletions())
                        columns.remove(b.as(CouchbaseColumnConverter.INSTANCE));
                }

                if (!columns.isEmpty()) {
                    updateColumns(document, columns);
                    return bucket.async().upsert(document); // TODO add PersistTo and ReplicateTo
                } else
                    return bucket.async().remove(document); // TODO add PersistTo and ReplicateTo
            })
            .last()
            .toBlocking()
            .single();

        sleepAfterWrite(txh, commitTime);
    }

    private void updateColumns(JsonDocument document, Map<String, CouchbaseColumn> columns) {
        final List<JsonObject> columnsList = columns.entrySet().stream().map(entry ->
            JsonObject.create()
                .put(CouchbaseColumn.KEY, entry.getKey())
                .put(CouchbaseColumn.VALUE, entry.getValue().getValue())
                .put(CouchbaseColumn.WRITE_TIME, entry.getValue().getWritetime())
                .put(CouchbaseColumn.TTL, entry.getValue().getTtl())
        ).collect(Collectors.toList());

        document.content().put(CouchbaseColumn.COLUMNS, JsonArray.from(columnsList));
    }

    private Map<String, CouchbaseColumn> getColumnsFromDocument(JsonDocument document, long currentTimeMillis) {
        final Map<String, CouchbaseColumn> columns = new HashMap<>();
        final Iterator it = document.content().getArray(CouchbaseColumn.COLUMNS).iterator();

        while (it.hasNext()) {
            JsonObject column = (JsonObject) it.next();
            long writetime = column.getLong(CouchbaseColumn.WRITE_TIME);
            int ttl = column.getInt(CouchbaseColumn.TTL);
            if (ttl == 0 || writetime + ttl > currentTimeMillis)
                columns.put(column.getString(CouchbaseColumn.KEY),
                    new CouchbaseColumn(column.getString(CouchbaseColumn.VALUE), writetime, ttl));
        }

        return columns;
    }

    private List<CouchbaseDocumentMutation> convertToDocumentMutations(Map<String, Map<StaticBuffer, KCVMutation>> batch) {
        List<CouchbaseDocumentMutation> documentMutations = new ArrayList<>();

        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> batchEntry : batch.entrySet()) {
            String table = batchEntry.getKey();
            Preconditions.checkArgument(openStores.containsKey(table), "Table cannot be found: " + table);

            Map<StaticBuffer, KCVMutation> mutations = batchEntry.getValue();
            for (Map.Entry<StaticBuffer, KCVMutation> ent : mutations.entrySet()) {
                KCVMutation mutation = ent.getValue();
                String id = CouchbaseColumnConverter.INSTANCE.toString(ent.getKey());
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
            CouchbaseKeyColumnValueStore newStore = new CouchbaseKeyColumnValueStore(this, bucketName,
                name, bucket);

            store = openStores.putIfAbsent(name, newStore);

            if (store == null) {
                store = newStore;
            }
        }

        return store;
    }

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig config) throws BackendException {
        return new CouchbaseTransaction(config);
    }

    @Override
    public String getName() {
        return bucketName;
    }

    /**
     * Deletes the specified table with all its columns.
     */
    @Override
    public void clearStorage() throws BackendException {
        try {
            bucket.query(deleteFrom(bucketName));
        } catch (Exception e) {
            throw new TemporaryBackendException(e);
        }
    }

    @Override
    public boolean exists() throws BackendException {
        return true;
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    private String determineTableName(org.janusgraph.diskstorage.configuration.Configuration config) {
        if ((!config.has(COUCHBASE_BUCKET)) && (config.has(GRAPH_NAME))) {
            return config.get(GRAPH_NAME);
        }
        return config.get(COUCHBASE_BUCKET);
    }

}
