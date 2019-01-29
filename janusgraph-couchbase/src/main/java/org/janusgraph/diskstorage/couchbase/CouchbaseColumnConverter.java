package org.janusgraph.diskstorage.couchbase;

import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;

import java.util.Arrays;


public class CouchbaseColumnConverter {//implements StaticBuffer.Factory<String> {
    public static final CouchbaseColumnConverter INSTANCE = new CouchbaseColumnConverter();
    private final static char[] hexArray = "0123456789ABCDEF".toCharArray();


//    @Override
//    public String get(byte[] array, int offset, int limit) {
//        byte[] source = getSource(array, offset, limit);
//        return toString(source);
//    }
//
//    public String toString(byte[] array) {
//        stringSerializer.
//
//
////        StaticBuffer sb = StaticArrayBuffer.of(array);
////        return KeyValueStoreUtil.getString(sb);
//        //return Base64.getEncoder().encodeToString(array);
//    }


    public String toString(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        String s = new String(hexChars);

        byte[] b = toByteArray(s);
        if (!Arrays.equals(bytes, b)) {
            System.out.println("fail");
        }

        return s;
    }

    public byte[] toByteArray(String value) {
//        final StaticBuffer buffer = toStaticBuffer(value);
//        return buffer.getBytes(0, buffer.length());

//        StaticBuffer sb = KeyValueStoreUtil.getBuffer(value);
//        String s = toString(sb);
//        System.out.println(s);
//        assert value.equals(s);
//        return sb.getBytes(0, sb.length());
        int len = value.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(value.charAt(i), 16) << 4)
                + Character.digit(value.charAt(i + 1), 16));
        }
        return data;
        //return Base64.getDecoder().decode(value);
    }

    public String toString(StaticBuffer buffer) {
        return toString(buffer.as(StaticBuffer.ARRAY_FACTORY));
        //return stringSerializer.read(buffer.asReadBuffer());
        // return KeyValueStoreUtil.getString(buffer);
        //return buffer.as(this);
    }

    public StaticBuffer toStaticBuffer(String value) {
        return StaticArrayBuffer.of(toByteArray(value));
//        WriteByteBuffer writeBuffer = new WriteByteBuffer();
//        stringSerializer.write(writeBuffer, value);
//        return writeBuffer.getStaticBuffer();
        //return KeyValueStoreUtil.getBuffer(value);
//        return new StaticArrayBuffer(toByteArray(value));
    }

//    private byte[] getSource(byte[] array, int offset, int limit) {
//        if (offset == 0 && limit == array.length)
//            return array;
//        else
//            return Arrays.copyOfRange(array, offset, limit);
//    }
}
