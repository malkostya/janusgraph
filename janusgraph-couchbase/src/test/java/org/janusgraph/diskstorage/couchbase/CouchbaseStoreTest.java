package org.janusgraph.diskstorage.couchbase;

import org.janusgraph.CouchbaseStorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyColumnValueStoreTest;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class CouchbaseStoreTest extends KeyColumnValueStoreTest {

    @BeforeClass
    public static void startCouchbase() throws IOException, BackendException {
        CouchbaseStorageSetup.startCouchbase();
    }

    public CouchbaseStoreManager openStorageManager(ModifiableConfiguration config) throws BackendException {
        return new CouchbaseStoreManager(new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,
            config.getConfiguration(), BasicConfiguration.Restriction.NONE));
    }

    public CouchbaseStoreManager openStorageManager() throws BackendException {
        return openStorageManager("");
    }

    public CouchbaseStoreManager openStorageManager(String bucketName) throws BackendException {
        return openStorageManager(bucketName, "");
    }

    public CouchbaseStoreManager openStorageManager(String bucketName, String graphName) throws BackendException {
        return new CouchbaseStoreManager(CouchbaseStorageSetup.getCouchbaseConfiguration(bucketName, graphName));
    }

    @Test
    public void testGetKeysWithKeyRange() throws Exception {
        super.testGetKeysWithKeyRange();
    }

    @Override
    public CouchbaseStoreManager openStorageManagerForClearStorageTest() throws Exception {
        return openStorageManager(CouchbaseStorageSetup.getCouchbaseConfiguration().set(
            GraphDatabaseConfiguration.DROP_ON_CLEAR, true));
    }

    @Test
    public void bucketShouldEqualSuppliedBucketName() throws BackendException {
        final CouchbaseStoreManager mgr = openStorageManager("randomBucketName");
        assertEquals("randomBucketName", mgr.getName());
    }

    @Test
    public void bucketShouldEqualGraphName() throws BackendException {
        final CouchbaseStoreManager mgr = openStorageManager("", "randomGraphName");
        assertEquals("randomGraphName", mgr.getName());
    }
}
