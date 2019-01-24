package org.janusgraph.diskstorage.couchbase;

import org.janusgraph.diskstorage.StaticBuffer;
import java.util.Arrays;
import java.util.Base64;

public class CouchbaseColumnConverter implements StaticBuffer.Factory<String> {
    public static final CouchbaseColumnConverter INSTANCE = new CouchbaseColumnConverter();

    @Override
    public String get(byte[] array, int offset, int limit) {
        byte[] source = getSource(array, offset, limit);
        return Base64.getEncoder().encodeToString(source);
    }

    public String toString(byte[] array) {
        return Base64.getEncoder().encodeToString(array);
    }

    public byte[] toByteArray(String value) {
        return Base64.getDecoder().decode(value);
    }

    private byte[] getSource(byte[] array, int offset, int limit) {
        if (offset==0 && limit==array.length)
            return array;
        else
            return Arrays.copyOfRange(array, offset, limit);
    }
}
