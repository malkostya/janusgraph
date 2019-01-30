package org.janusgraph.graphdb.couchbase;

import org.janusgraph.CouchbaseStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphPerformanceMemoryTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

public class CouchbaseGraphPerformanceMemoryTest extends JanusGraphPerformanceMemoryTest {
    @BeforeClass
    public static void startCouchbase() throws IOException {
        CouchbaseStorageSetup.startCouchbase();
    }

    @AfterClass
    public static void stopCouchbase() {
        CouchbaseStorageSetup.killIfRunning();
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return CouchbaseStorageSetup.getCouchbaseGraphConfiguration();
    }

}
