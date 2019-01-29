package org.janusgraph.diskstorage.couchbase;

public class CouchbaseColumn {
    // attributes keys of json document
    public static final String ID = "id";
    public static final String TABLE = "table";
    public static final String COLUMNS = "columns";
    public static final String KEY = "key";
    public static final String VALUE = "value";
    public static final String EXPIRE = "expire";
    public static final String TTL = "ttl";
    // instance members
    private String value;
    private long expire;
    private int ttl;

    public CouchbaseColumn(String value, long expire, int ttl) {
        this.value = value;
        this.expire = expire;
        this.ttl = ttl;
    }

    public String getValue() {
        return value;
    }

    public long getExpire() {
        return expire;
    }

    public int getTtl() {
        return ttl;
    }
}
