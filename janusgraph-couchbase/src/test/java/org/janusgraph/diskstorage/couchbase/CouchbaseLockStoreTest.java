package org.janusgraph.diskstorage.couchbase;

import org.janusgraph.CouchbaseStorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.LockKeyColumnValueStoreTest;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

public class CouchbaseLockStoreTest extends LockKeyColumnValueStoreTest {

    @BeforeClass
    public static void startCouchbase() throws IOException {
        CouchbaseStorageSetup.startCouchbase();
    }

    @AfterClass
    public static void stopCouchbase() {
        CouchbaseStorageSetup.killIfRunning();
    }

    public KeyColumnValueStoreManager openStorageManager(int idx, Configuration configuration) throws BackendException {
        return new CouchbaseStoreManager(CouchbaseStorageSetup.getCouchbaseConfiguration());
    }
}
