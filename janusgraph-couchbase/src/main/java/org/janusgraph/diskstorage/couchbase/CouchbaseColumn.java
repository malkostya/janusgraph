package org.janusgraph.diskstorage.couchbase;

public class CouchbaseColumn implements Comparable<CouchbaseColumn> {
    // attributes keys of json document
    public static final String ID = "id";
    public static final String TABLE = "table";
    public static final String COLUMNS = "columns";
    public static final String KEY = "key";
    public static final String VALUE = "value";
    public static final String EXPIRE = "expire";
    public static final String TTL = "ttl";
    // instance members
    private String key;
    private String value;
    private long expire;
    private int ttl;

    public CouchbaseColumn(String key, String value, long expire, int ttl) {
        this.key = key;
        this.value = value;
        this.expire = expire;
        this.ttl = ttl;
    }

    public String getKey() {
        return key;
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

    public int compareTo(CouchbaseColumn o) {
        return key.compareTo(o.key);
    }

    public boolean equals(Object anObject) {
        if (this == anObject) {
            return true;
        }
        if (anObject instanceof CouchbaseColumn) {
            CouchbaseColumn anotherColumn = (CouchbaseColumn)anObject;
            return key.equals(anotherColumn.key);
        }
        return false;
    }

    public int hashCode() {
        return key.hashCode();
    }
}
