package org.janusgraph.diskstorage.couchbase;

public class CouchbaseColumn {
    private String value;
    private long expire;

    public CouchbaseColumn(String value, long expire) {
        this.value = value;
        this.expire = expire;
    }

    public String getValue() {
        return value;
    }

    public long getExpire() {
        return expire;
    }
}
