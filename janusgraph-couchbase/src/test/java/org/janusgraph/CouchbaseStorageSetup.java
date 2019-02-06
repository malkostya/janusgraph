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

package org.janusgraph;

import org.apache.commons.lang3.StringUtils;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.couchbase.BucketWrapper;
import org.janusgraph.diskstorage.couchbase.CouchbaseStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CouchbaseStorageSetup {

    private static final Logger log = LoggerFactory.getLogger(CouchbaseStorageSetup.class);

    private static final String DEFAULT_USERNAME = "janusgraph";

    private static final String DEFAULT_PASSWORD = "janusgraph";


    // Couchbase config for testing
//
//    public static final String HBASE_PARENT_DIR_PROP = "test.hbase.parentdir";
//
//    private static final Pattern HBASE_SUPPORTED_VERSION_PATTERN = Pattern.compile("^((2\\.[01])|(1\\.[01234]))\\..*");
//
//    private static final String HBASE_VERSION_1_STRING = "1.";
//
//    private static final String HBASE_PARENT_DIR;
//
//    private static final String HBASE_TARGET_VERSION = VersionInfo.getVersion();
//
//    private static final HBaseCompat compat;
//
//    static {
//        String parentDir = "..";
//        String tmp = System.getProperty(HBASE_PARENT_DIR_PROP);
//        if (null != tmp) {
//            parentDir = tmp;
//        }
//        HBASE_PARENT_DIR = parentDir;
//        compat = HBaseCompatLoader.getCompat(null);
//    }
//
//    private static final String HBASE_STAT_FILE = "/tmp/janusgraph-hbase-test-daemon.stat";
//
//    private volatile static HBaseStatus HBASE = null;
//
//    public static String getScriptDirForHBaseVersion(String hv) {
//        return getDirForHBaseVersion(hv, "bin");
//    }
//
//    public static String getConfDirForHBaseVersion(String hv) {
//        return getDirForHBaseVersion(hv, "conf");
//    }
//
//    public static String getDirForHBaseVersion(String hv, String lastSubdirectory) {
//        Matcher m = HBASE_SUPPORTED_VERSION_PATTERN.matcher(hv);
//        if (m.matches()) {
//            String result = String.format("%s%sjanusgraph-hbase-server/%s/", HBASE_PARENT_DIR, File.separator, lastSubdirectory);
//            log.debug("Built {} path for HBase version {}: {}", lastSubdirectory, hv, result);
//            return result;
//        } else {
//            throw new RuntimeException("Unsupported HBase test version " + hv + " does not match pattern " + HBASE_SUPPORTED_VERSION_PATTERN);
//        }
//    }

    public static ModifiableConfiguration getCouchbaseConfiguration() {
        return getCouchbaseConfiguration("");
    }

    public static ModifiableConfiguration getCouchbaseConfiguration(String tableName) {
        return getCouchbaseConfiguration(tableName, "");
    }

    public static ModifiableConfiguration getCouchbaseConfiguration(String bucketName, String graphName) {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.AUTH_USERNAME, DEFAULT_USERNAME);
        config.set(GraphDatabaseConfiguration.AUTH_PASSWORD, DEFAULT_PASSWORD);
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "couchbase");
        if (!StringUtils.isEmpty(bucketName)) config.set(BucketWrapper.COUCHBASE_BUCKET, bucketName);
        if (!StringUtils.isEmpty(graphName)) config.set(GraphDatabaseConfiguration.GRAPH_NAME, graphName);
        config.set(GraphDatabaseConfiguration.TIMESTAMP_PROVIDER, CouchbaseStoreManager.PREFERRED_TIMESTAMPS);
        config.set(GraphDatabaseConfiguration.DROP_ON_CLEAR, false);

        return config;
    }

    public static WriteConfiguration getCouchbaseGraphConfiguration() {
        return getCouchbaseConfiguration().getConfiguration();
    }

    public synchronized static void startCouchbase() throws IOException {
//        if (HBASE != null) {
//            log.info("HBase already started");
//            return HBASE;
//        }
//
//        killIfRunning();
//
//        deleteData();
//
//        log.info("Starting HBase");
//        String scriptPath = getScriptDirForHBaseVersion(HBASE_TARGET_VERSION) + "/hbase-daemon.sh";
//        DaemonRunner.runCommand(scriptPath, "--config", getConfDirForHBaseVersion(HBASE_TARGET_VERSION), "start", "master");
//
//        HBASE = HBaseStatus.write(HBASE_STAT_FILE, HBASE_TARGET_VERSION);
//
//        registerKillerHook(HBASE);
//
//        waitForConnection(60L, TimeUnit.SECONDS);
//
//        return HBASE;
    }

    public synchronized static void killIfRunning() {
//        HBaseStatus stat = HBaseStatus.read(HBASE_STAT_FILE);
//
//        if (null == stat) {
//            log.info("HBase is not running");
//            return;
//        }
//
//        shutdownCouchbase(stat);
    }

//    public synchronized static void waitForConnection(long timeout, TimeUnit timeoutUnit) {
//        long before = System.currentTimeMillis();
//        long after;
//        long timeoutMS = TimeUnit.MILLISECONDS.convert(timeout, timeoutUnit);
//        do {
//            try (ConnectionMask hc = compat.createConnection(HBaseConfiguration.create())) {
//                after = System.currentTimeMillis();
//                log.info("HBase server to started after about {} ms", after - before);
//                return;
//            } catch (IOException e) {
//                log.info("Exception caught while waiting for the HBase server to start", e);
//            }
//            after = System.currentTimeMillis();
//        } while (timeoutMS > after - before);
//        after = System.currentTimeMillis();
//        log.warn("HBase server did not start in {} ms", after - before);
//    }
//
//    /**
//     * Delete HBase data under the current working directory.
//     */
//    private synchronized static void deleteData() {
//        try {
//            // please keep in sync with HBASE_CONFIG_DIR/hbase-site.xml, reading HBase XML config is huge pain.
//            File hbaseRoot = new File("./target/hbase-root");
//            File zookeeperDataDir = new File("./target/zk-data");
//
//            if (hbaseRoot.exists()) {
//                log.info("Deleting {}", hbaseRoot);
//                FileUtils.deleteDirectory(hbaseRoot);
//            }
//
//            if (zookeeperDataDir.exists()) {
//                log.info("Deleting {}", zookeeperDataDir);
//                FileUtils.deleteDirectory(zookeeperDataDir);
//            }
//
//        } catch (IOException e) {
//            throw new RuntimeException("Failed to delete old HBase test data directories", e);
//        }
//    }
//
//    /**
//     * Register a shutdown hook with the JVM that attempts to kill the external
//     * HBase daemon
//     *
//     * @param stat
//     *            the HBase daemon to kill
//     */
//    private static void registerKillerHook(final HBaseStatus stat) {
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdownCouchbase(stat)));
//    }

    private synchronized static void shutdownCouchbase(){//HBaseStatus stat) {
//
//        log.info("Shutting down HBase...");
//
//        // First try graceful shutdown through the script...
//        DaemonRunner.runCommand(stat.getScriptDir() + "/hbase-daemon.sh", "--config", stat.getConfDir(), "stop", "master");
//
//        log.info("Shutdown HBase");
//
//        if (!stat.getFile().delete()) {
//            log.warn("Unable to delete stat file {}", stat.getFile().getAbsolutePath());
//        }
//
//        log.info("Deleted {}", stat.getFile());
//
//        HBASE = null;
    }
    
    /**
     * Create a snapshot for a table.
     * @param snapshotName
     * @param table
     * @throws BackendException 
     */
//    public synchronized static void createSnapshot(String snapshotName, String table)
//            throws BackendException {
//        try (ConnectionMask hc = compat.createConnection(HBaseConfiguration.create());
//                AdminMask admin = hc.getAdmin()) {
//            admin.snapshot(snapshotName, table);
//        } catch (Exception e) {
//            log.warn("Create HBase snapshot failed", e);
//            throw new TemporaryBackendException("Create HBase snapshot failed", e);
//        }
//    }
//
//    /**
//     * Delete a snapshot.
//     * @param snapshotName
//     * @throws IOException
//     */
//    public synchronized static void deleteSnapshot(String snapshotName) throws IOException {
//        try (ConnectionMask hc = compat.createConnection(HBaseConfiguration.create());
//                AdminMask admin = hc.getAdmin()) {
//            admin.deleteSnapshot(snapshotName);
//        }
//    }
//
//    /**
//     * Return the hbase root dir
//     */
//    public static String getHBaseRootdir() {
//        return getDirForHBaseVersion(HBASE_TARGET_VERSION, "target/hbase-root");
//    }
}
