package org.janusgraph.diskstorage.couchbase;

public class CouchbaseColumn {
    private String value;
    private long expired;

    public CouchbaseColumn(String value, long expired) {
        this.value = value;
        this.expired = expired;
    }

    public String getValue() {
        return value;
    }

    public long getExpired() {
        return expired;
    }
}
