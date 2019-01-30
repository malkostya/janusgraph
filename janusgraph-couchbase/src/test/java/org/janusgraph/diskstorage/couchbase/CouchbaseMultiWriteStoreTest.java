package org.janusgraph.diskstorage.couchbase;

import org.janusgraph.CouchbaseStorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.MultiWriteKeyColumnValueStoreTest;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

public class CouchbaseMultiWriteStoreTest extends MultiWriteKeyColumnValueStoreTest {

    @BeforeClass
    public static void startCouchbase() throws IOException {
        CouchbaseStorageSetup.startCouchbase();
    }

    @AfterClass
    public static void stopCopuchbase() {
        CouchbaseStorageSetup.killIfRunning();
    }

    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        return new CouchbaseStoreManager(CouchbaseStorageSetup.getCouchbaseConfiguration());
    }
}
