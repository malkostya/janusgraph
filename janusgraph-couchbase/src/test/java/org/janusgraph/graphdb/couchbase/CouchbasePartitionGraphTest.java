package org.janusgraph.graphdb.couchbase;

import org.janusgraph.CouchbaseStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphPartitionGraphTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

public class CouchbasePartitionGraphTest extends JanusGraphPartitionGraphTest {

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return CouchbaseStorageSetup.getCouchbaseGraphConfiguration();
    }

    @AfterClass
    public static void stopCouchbase() {
        CouchbaseStorageSetup.killIfRunning();
    }

    @BeforeClass
    public static void startCouchbase() throws IOException {
        CouchbaseStorageSetup.startCouchbase();
    }

}
