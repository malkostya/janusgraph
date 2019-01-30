package org.janusgraph.graphdb.couchbase;

import org.janusgraph.CouchbaseStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphTest;
import org.junit.BeforeClass;

import java.io.IOException;

public class CouchbaseGraphTest extends JanusGraphTest {
    @BeforeClass
    public static void startCouchbase() throws IOException {
        CouchbaseStorageSetup.startCouchbase();
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return CouchbaseStorageSetup.getCouchbaseGraphConfiguration();
    }
}
