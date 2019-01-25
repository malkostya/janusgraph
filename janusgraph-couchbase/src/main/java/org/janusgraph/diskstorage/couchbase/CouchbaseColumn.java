package org.janusgraph.diskstorage.couchbase;

public class CouchbaseColumn {
    // attributes keys of json document
    public static final String ID = "id";
    public static final String TABLE = "table";
    public static final String COLUMNS = "columns";
    public static final String KEY = "key";
    public static final String VALUE = "value";
    public static final String WRITE_TIME = "writetime";
    public static final String TTL = "ttl";
    // instance members
    private String value;
    private long writetime;
    private int ttl;

    public CouchbaseColumn(String value, long writetime, int ttl) {
        this.value = value;
        this.writetime = writetime;
        this.ttl = ttl;
    }

    public String getValue() {
        return value;
    }

    public long getWritetime() {
        return writetime;
    }

    public int getTtl() {
        return ttl;
    }
}
