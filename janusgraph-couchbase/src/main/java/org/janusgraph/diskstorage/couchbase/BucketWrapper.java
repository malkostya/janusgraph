package org.janusgraph.diskstorage.couchbase;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.Statement;
import org.apache.commons.collections.CollectionUtils;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import static com.couchbase.client.java.query.Delete.deleteFrom;
import static org.janusgraph.diskstorage.couchbase.CouchbaseStoreManager.COUCHBASE_NS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;

public class BucketWrapper {

    private static final Logger logger = LoggerFactory.getLogger(BucketWrapper.class);

    public static final ConfigOption<String> COUCHBASE_BUCKET =
        new ConfigOption<>(COUCHBASE_NS, "bucket",
            "The name of the bucket JanusGraph will use. JanusGraph is not able to create a new bucket but" +
                " should use predefined one." +
                " If this configuration option is not provided but graph.graphname is, the bucket will be set" +
                " to that value.",
            ConfigOption.Type.LOCAL, "janusgraph");

    public static final ConfigOption<Integer> RAM_QUOTA_MB =
        new ConfigOption<Integer>(COUCHBASE_NS, "ram-quota-mb",
            "Required parameter. RAM Quota for new bucket in MB. Numeric. The minimum you can specify is" +
                " 100, and the maximum can only be as great as the memory quota established for the node. If other" +
                " buckets are associated with a node, RAM Quota can only be as large as the amount memory remaining" +
                " for the node, accounting for the other bucket memory quota.",
            ConfigOption.Type.MASKABLE, Integer.class, input -> null != input && 100 <= input);

    private final Cluster cluster;
    private final BucketHelper bucketHelper;
    private final String bucketName;
    private final int ramQuotaMB;
    private Bucket bucket;

    public BucketWrapper(org.janusgraph.diskstorage.configuration.Configuration config,
                         String[] hostnames,
                         int port,
                         String username,
                         String password) {
        bucketName = determineBucketName(config);
        ramQuotaMB = config.has(RAM_QUOTA_MB) ? config.get(RAM_QUOTA_MB) : 100;

        cluster = CouchbaseCluster.create(hostnames[0] + ":" + port);
        cluster.authenticate(username, password);
        bucketHelper = new BucketHelper(hostnames[0], port, username, password);
    }

    public void close() {
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

        try {
            bucketHelper.close();
        } catch (CouchbaseException e) {
            logger.warn("Failed closing bucketHelper", e);
        }
    }

    public Observable<JsonDocument> asyncUpsert(JsonDocument document) {
        return bucket.async().upsert(document); // TODO add PersistTo and ReplicateTo
    }

    public Observable<JsonDocument> asyncRemove(JsonDocument document) {
        return bucket.async().remove(document); // TODO add PersistTo and ReplicateTo
    }

    public void upsert(JsonDocument document) {
        bucket.upsert(document); // TODO add PersistTo and ReplicateTo
    }

    public void remove(JsonDocument document) {
        bucket.remove(document); // TODO add PersistTo and ReplicateTo
    }

    public JsonDocument get(String documentId) {
        return bucket.get(documentId); // TODO add getAndLock option to enforce consistency
    }

    public N1qlQueryResult query(String statement) {
        return query(N1qlQuery.simple(statement));
    }

    public N1qlQueryResult query(Statement statement) {
        return query(N1qlQuery.simple(statement));
    }

    public N1qlQueryResult query(N1qlQuery n1qlQuery) {
        final N1qlQueryResult queryResult = bucket.query(n1qlQuery);

        if (!queryResult.finalSuccess() || CollectionUtils.isNotEmpty(queryResult.errors())) {
            throw new CouchbaseException("Error executing N1QL statement '" + n1qlQuery.statement() + "'. "
                + queryResult.errors());
        }

        return queryResult;
    }

    public String getBucketName() {
        return bucketName;
    }

    public boolean bucketExists() {
        return bucketHelper.exists(bucketName);
    }

    public void ensureBucketExists() {
        if (!bucketExists())
            createBucket();
        else if (bucket == null)
            bucket = cluster.openBucket(bucketName);
    }

    public void dropBucket() {
        bucketHelper.drop(bucketName);
    }

    public void truncateBucket() {
        query(deleteFrom(bucketName)); // TODO replace to truncate
    }

    private void createBucket() {
        bucketHelper.create(bucketName, "couchbase", ramQuotaMB);
        bucket = cluster.openBucket(bucketName);
        query("CREATE PRIMARY INDEX `" + bucketName + "_primary` ON `" + bucketName
            + "` WITH { \"defer_build\":false }");
    }

    private String determineBucketName(org.janusgraph.diskstorage.configuration.Configuration config) {
        if ((!config.has(COUCHBASE_BUCKET)) && (config.has(GRAPH_NAME))) {
            return config.get(GRAPH_NAME);
        }
        return config.get(COUCHBASE_BUCKET);
    }
}
