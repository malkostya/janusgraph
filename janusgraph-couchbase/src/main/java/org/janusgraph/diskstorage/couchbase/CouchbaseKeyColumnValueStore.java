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

package org.janusgraph.diskstorage.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import org.janusgraph.util.system.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

/**
 *
 */
public class CouchbaseKeyColumnValueStore implements KeyColumnValueStore {

    private static final Logger logger = LoggerFactory.getLogger(CouchbaseKeyColumnValueStore.class);

    private final String bucketName;
    private final Bucket bucket;
    private final CouchbaseStoreManager storeManager;
    private final CouchbaseGetter entryGetter;
    private final String table;

    CouchbaseKeyColumnValueStore(CouchbaseStoreManager storeManager, String bucketName, String table, Bucket bucket) {
        this.storeManager = storeManager;
        this.bucketName = bucketName;
        this.bucket = bucket;
        this.table = table;
        this.entryGetter = new CouchbaseGetter(storeManager.getMetaDataSchema(this.table));
    }

    @Override
    public void close() throws BackendException {
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        final List<N1qlQueryRow> rows = query(Collections.singletonList(query.getKey()), null, null,
            query.getSliceStart(), query.getSliceEnd(), query.hasLimit() ? query.getLimit() : 0);

        if (rows.isEmpty())
            return EntryList.EMPTY_LIST;
        else if (rows.size() == 1) {
            final JsonArray columns = rows.get(0).value().getArray(CouchbaseColumn.COLUMNS);
            return StaticArrayEntryList.ofByteBuffer(columns.iterator(), entryGetter);
        } else
            throw new TemporaryBackendException("Multiple rows with the same key.");
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh)
        throws BackendException {
        final List<N1qlQueryRow> rows = query(keys, null, null,
            query.getSliceStart(), query.getSliceEnd(), getLimit(query));

        return rows.stream().collect(Collectors.toMap(
            row -> CouchbaseColumnConverter.INSTANCE.toStaticBuffer(row.value().getString(CouchbaseColumn.ID)),
            row -> StaticArrayEntryList.ofByteBuffer(row.value().getArray(CouchbaseColumn.COLUMNS).iterator(),
                entryGetter)
        ));
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh)
        throws BackendException {
        Map<StaticBuffer, KCVMutation> mutations = ImmutableMap.of(key, new KCVMutation(additions, deletions));
        storeManager.mutateMany(ImmutableMap.of(table, mutations), txh);
    }

    @Override
    public void acquireLock(StaticBuffer key,
                            StaticBuffer column,
                            StaticBuffer expectedValue,
                            StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return table;
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {
        return executeKeySliceQuery(query.getKeyStart(), query.getKeyEnd(), query.getSliceStart(), query.getSliceEnd(),
            getLimit(query));
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
        return executeKeySliceQuery(null, null, query.getSliceStart(), query.getSliceEnd(),
            getLimit(query));
    }

    private KeyIterator executeKeySliceQuery(StaticBuffer keyStart, StaticBuffer keyEnd,
                                             StaticBuffer sliceStart, StaticBuffer sliceEnd, int limit) {

        final List<N1qlQueryRow> rows = query(null, keyStart, keyEnd, sliceStart, sliceEnd, limit);

        // TODO

        return null;
    }

    private List<N1qlQueryRow> query(List<StaticBuffer> keys, StaticBuffer keyStart, StaticBuffer keyEnd,
                                     StaticBuffer sliceStart, StaticBuffer sliceEnd, int limit) {
        final long currentTimeMillis = storeManager.currentTimeMillis();
        final StringBuilder select = new StringBuilder("SELECT");
        final StringBuilder where = new StringBuilder(" WHERE table = $table");
        final JsonObject placeholderValues = JsonObject.create()
            .put("table", table)
            .put("curtime", currentTimeMillis);

        if (keys != null) {
            if (keys.size() == 1) {
                where.append(" AND meta().id = $key");
                placeholderValues.put("key", CouchbaseColumnConverter.INSTANCE.toString(keys.get(0)));
            } else {
                select.append(" meta().id AS id,");
                where.append(" AND meta().id IN [");
                for (StaticBuffer key : keys)
                    where.append(CouchbaseColumnConverter.INSTANCE.toString(key)).append(", ");
                where.delete(where.length() - 2, where.length()).append("]");
            }
        } else {
            select.append(" meta().id AS id,");

            if (keyStart != null) {
                where.append(" AND meta().id >= $keyStart");
                placeholderValues.put("keyStart", CouchbaseColumnConverter.INSTANCE.toString(keyStart));
            }

            if (keyEnd != null) {
                where.append(" AND meta().id < $keyEnd");
                placeholderValues.put("keyEnd", CouchbaseColumnConverter.INSTANCE.toString(keyEnd));
            }
        }

        select.append(" ARRAY a FOR a IN columns WHEN a.writetime + a.ttl > $curtime"); // TODO change to '> ").append(currentTimeMillis)' if it's not working
        where.append(" AND ANY a IN columns SATISFIES a.writetime + a.ttl > $curtime");


        if (sliceStart != null) {
            final String sliceStartString = CouchbaseColumnConverter.INSTANCE.toString(sliceStart);
            select.append(" AND a.key >= $sliceStart"); // TODO change to '>= ").append(sliceStartString)' if it's not working
            where.append(" AND a.key >= $sliceStart");
            placeholderValues.put("$sliceStart", sliceStartString);
        }

        if (sliceEnd != null) {
            final String sliceEndString = CouchbaseColumnConverter.INSTANCE.toString(sliceEnd);
            select.append(" AND a.key < $sliceEnd"); // TODO change to '< ").append(sliceEndString)' if it's not working
            where.append(" AND a.key < $sliceEnd");
            placeholderValues.put("$sliceEnd", sliceEndString);
        }

        select.append(" END as columns");
        where.append(" END");

        if (limit > 0) {
            where.append(" LIMIT $limit");
            placeholderValues.put("limit", limit);
        }

        final N1qlQuery n1qlQuery = N1qlQuery.parameterized(
            select.append(" FROM `").append(bucketName).append("`").append(where).toString(),
            placeholderValues);

        return bucket.query(n1qlQuery).allRows();
    }

    private int getLimit(SliceQuery query) {
        return query.hasLimit() ? query.getLimit() : 0;
    }

    private static class CouchbaseGetter implements StaticArrayEntry.GetColVal<Object, ByteBuffer> {

        private final EntryMetaData[] schema;

        private CouchbaseGetter(EntryMetaData[] schema) {
            this.schema = schema;
        }

        @Override
        public ByteBuffer getColumn(Object element) {
            return asByteBuffer(element, CouchbaseColumn.KEY);
        }

        @Override
        public ByteBuffer getValue(Object element) {
            return asByteBuffer(element, CouchbaseColumn.VALUE);
        }

        @Override
        public EntryMetaData[] getMetaSchema(Object element) {
            return schema;
        }

        @Override
        public Object getMetaData(Object element, EntryMetaData meta) {
            switch (meta) {
                case TIMESTAMP:
                    return ((JsonObject) element).getLong(CouchbaseColumn.WRITE_TIME);
                case TTL:
                    return ((JsonObject) element).getInt(CouchbaseColumn.TTL);
                default:
                    throw new UnsupportedOperationException("Unsupported meta data: " + meta);
            }
        }

        private ByteBuffer asByteBuffer(Object element, String elementKey) {
            final String elementValue = ((JsonObject) element).getString(elementKey);
            return CouchbaseColumnConverter.INSTANCE.toByteBuffer(elementValue);
        }
    }

    private class RowIterator implements KeyIterator {
        private final Closeable table;
        private final Iterator<Result> rows;
        private final byte[] columnFamilyBytes;

        private Result currentRow;
        private boolean isClosed;

        public RowIterator(Closeable table, ResultScanner rows, byte[] columnFamilyBytes) {
            this.table = table;
            this.columnFamilyBytes = Arrays.copyOf(columnFamilyBytes, columnFamilyBytes.length);
            this.rows = Iterators.filter(rows.iterator(), result -> null != result && null != result.getRow());
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            ensureOpen();

            return new RecordIterator<Entry>() {
                private final Iterator<Map.Entry<byte[], NavigableMap<Long, byte[]>>> kv;

                {
                    final Map<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = currentRow.getMap();
                    Preconditions.checkNotNull(map);
                    kv = map.get(columnFamilyBytes).entrySet().iterator();
                }

                @Override
                public boolean hasNext() {
                    ensureOpen();
                    return kv.hasNext();
                }

                @Override
                public Entry next() {
                    ensureOpen();
                    return StaticArrayEntry.ofBytes(kv.next(), entryGetter);
                }

                @Override
                public void close() {
                    isClosed = true;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public boolean hasNext() {
            ensureOpen();
            return rows.hasNext();
        }

        @Override
        public StaticBuffer next() {
            ensureOpen();

            currentRow = rows.next();
            return StaticArrayBuffer.of(currentRow.getRow());
        }

        @Override
        public void close() {
            IOUtils.closeQuietly(table);
            isClosed = true;
            logger.debug("RowIterator closed table {}", table);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private void ensureOpen() {
            if (isClosed)
                throw new IllegalStateException("Iterator has been closed.");
        }
    }
}
