package org.janusgraph.diskstorage.couchbase;

import org.janusgraph.diskstorage.StaticBuffer;

import java.util.Arrays;

public class CouchbaseColumnConverter implements StaticBuffer.Factory<String> {
    public static final CouchbaseColumnConverter INSTANCE = new CouchbaseColumnConverter();

    @Override
    public String get(byte[] array, int offset, int limit) {
        byte[] source = getSource(array, offset, limit);
        return java.util.Base64.getEncoder().encodeToString(source);
    }

    private byte[] getSource(byte[] array, int offset, int limit) {
        if (offset==0 && limit==array.length)
            return array;
        else
            return Arrays.copyOfRange(array,offset,limit);
    }
}
