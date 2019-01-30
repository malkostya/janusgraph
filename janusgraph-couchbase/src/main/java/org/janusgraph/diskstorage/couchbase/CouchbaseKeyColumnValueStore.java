package org.janusgraph.diskstorage.couchbase;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import com.google.common.collect.Iterators;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class CouchbaseKeyColumnValueStore implements KeyColumnValueStore {
    private static final Logger logger = LoggerFactory.getLogger(CouchbaseKeyColumnValueStore.class);
    private static final CouchbaseColumnConverter columnConverter = CouchbaseColumnConverter.INSTANCE;
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
    public void close() {
    }

    public static void main(String[] args) {
        System.out.println("abc123".compareTo("ABC223"));

        byte[] b = new byte[]{
            0, 0, 0, 0, 0, 0, 3, -24
        };
        String s = columnConverter.toString(b);
        System.out.println(s);
        byte[] b1 = columnConverter.toByteArray(s);

        System.out.println(b1);
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        final List<N1qlQueryRow> rows = query(Collections.singletonList(query.getKey()), null, null,
            query.getSliceStart(), query.getSliceEnd()).allRows();

        if (rows.isEmpty())
            return EntryList.EMPTY_LIST;
        else if (rows.size() == 1) {
            final JsonArray columns = rows.get(0).value().getArray(CouchbaseColumn.COLUMNS);
            return StaticArrayEntryList.ofBytes(convertAndSortColumns(columns, getLimit(query)), entryGetter);
        } else
            throw new TemporaryBackendException("Multiple rows with the same key.");
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh)
        throws BackendException {
        final List<N1qlQueryRow> rows = query(keys, null, null,
            query.getSliceStart(), query.getSliceEnd()).allRows();

        return rows.stream().collect(Collectors.toMap(
            row -> getRowId(row),
            row -> StaticArrayEntryList.ofBytes(convertAndSortColumns(row.value().getArray(CouchbaseColumn.COLUMNS),
                getLimit(query)), entryGetter)
        ));
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh)
        throws BackendException {
        final String documentId = columnConverter.toString(key);
        logger.info("MUTATE ROWID=" + documentId);
        final CouchbaseDocumentMutation docMutation = new CouchbaseDocumentMutation(table, documentId,
            new KCVMutation(additions, deletions));
        storeManager.mutate(docMutation, txh);
    }

    @Override
    public void acquireLock(StaticBuffer key,
                            StaticBuffer column,
                            StaticBuffer expectedValue,
                            StoreTransaction txh) {
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

    private KeyIterator executeKeySliceQuery(StaticBuffer keyStart, StaticBuffer keyEnd, StaticBuffer sliceStart,
                                             StaticBuffer sliceEnd, int limit) throws BackendException {
        final N1qlQueryResult queryResult = query(null, keyStart, keyEnd, sliceStart, sliceEnd);
        return new RowIterator(queryResult.iterator(), limit);
    }

    private N1qlQueryResult query(List<StaticBuffer> keys, StaticBuffer keyStart, StaticBuffer keyEnd,
                                  StaticBuffer sliceStart, StaticBuffer sliceEnd)
        throws BackendException {
        final long currentTimeMillis = storeManager.currentTimeMillis();
        final StringBuilder select = new StringBuilder("SELECT");
        final StringBuilder where = new StringBuilder(" WHERE table = $table");
        final JsonObject placeholderValues = JsonObject.create()
            .put("table", table)
            .put("curtime", currentTimeMillis);

        if (keys != null) {
            if (keys.size() == 1) {
                where.append(" AND meta().id = $key");
                placeholderValues.put("key", columnConverter.toString(keys.get(0)));
            } else {
                select.append(" meta().id AS id,");
                where.append(" AND meta().id IN [");
                for (StaticBuffer key : keys)
                    where.append(columnConverter.toString(key)).append(", ");
                where.delete(where.length() - 2, where.length()).append("]");
            }
        } else {
            select.append(" meta().id AS id,");

            if (keyStart != null) {
                where.append(" AND meta().id >= $keyStart");
                placeholderValues.put("keyStart", columnConverter.toString(keyStart));
            }

            if (keyEnd != null) {
                where.append(" AND meta().id < $keyEnd");
                placeholderValues.put("keyEnd", columnConverter.toString(keyEnd));
            }
        }

        select.append(" ARRAY a FOR a IN columns WHEN a.`expire` > $curtime");
        where.append(" AND ANY a IN columns SATISFIES a.`expire` > $curtime");


        if (sliceStart != null) {
            final String sliceStartString = columnConverter.toString(sliceStart);
            select.append(" AND a.`key` >= $sliceStart");
            where.append(" AND a.`key` >= $sliceStart");
            placeholderValues.put("$sliceStart", sliceStartString);
        }

        if (sliceEnd != null) {
            final String sliceEndString = columnConverter.toString(sliceEnd);
            select.append(" AND a.`key` < $sliceEnd");
            where.append(" AND a.`key` < $sliceEnd");
            placeholderValues.put("$sliceEnd", sliceEndString);
        }

        select.append(" END as columns");
        where.append(" END");

        final N1qlParams params = N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS); // TODO change to AtPlus

        final N1qlQuery n1qlQuery = N1qlQuery.parameterized(
            select.append(" FROM `").append(bucketName).append("`").append(where).toString(),
            placeholderValues, params);

        try {
            return bucket.query(n1qlQuery);
        } catch (CouchbaseException e) {
            throw new TemporaryBackendException(e);
        }
    }

    private StaticBuffer getRowId(N1qlQueryRow row) {
        return columnConverter.toStaticBuffer(row.value().getString(CouchbaseColumn.ID));
    }

    private int getLimit(SliceQuery query) {
        return query.hasLimit() ? query.getLimit() : 0;
    }

    private List<CouchbaseColumn> convertAndSortColumns(JsonArray columnsArray, int limit) {
        final Iterator itr = columnsArray.iterator();
        final List<CouchbaseColumn> columns = new ArrayList<>(columnsArray.size());

        while (itr.hasNext()) {
            final JsonObject column = (JsonObject) itr.next();
            columns.add(new CouchbaseColumn(
                column.getString(CouchbaseColumn.KEY),
                column.getString(CouchbaseColumn.VALUE),
                column.getLong(CouchbaseColumn.EXPIRE),
                column.getInt(CouchbaseColumn.TTL)));
        }

        columns.sort(Comparator.naturalOrder());

        return limit ==0 || limit >= columns.size() ? columns : columns.subList(0, limit);
    }

    private static class CouchbaseGetter implements StaticArrayEntry.GetColVal<CouchbaseColumn, byte[]> {

        private static final CouchbaseColumnConverter columnConverter = CouchbaseColumnConverter.INSTANCE;
        private final EntryMetaData[] schema;

        private CouchbaseGetter(EntryMetaData[] schema) {
            this.schema = schema;
        }

        @Override
        public byte[] getColumn(CouchbaseColumn column) {
            return columnConverter.toByteArray(column.getKey());
        }

        @Override
        public byte[] getValue(CouchbaseColumn column) {
            return columnConverter.toByteArray(column.getValue());
        }

        @Override
        public EntryMetaData[] getMetaSchema(CouchbaseColumn column) {
            return schema;
        }

        @Override
        public Object getMetaData(CouchbaseColumn column, EntryMetaData meta) {
            switch (meta) {
                case TIMESTAMP:
                    return column.getExpire() - column.getTtl() * 1000L;
                case TTL:
                    final int ttl = column.getTtl();
                    return ttl == Integer.MAX_VALUE ? 0 : ttl;
                default:
                    throw new UnsupportedOperationException("Unsupported meta data: " + meta);
            }
        }
    }

    private class RowIterator implements KeyIterator {
        private final Iterator<N1qlQueryRow> rows;
        private N1qlQueryRow currentRow;
        private boolean isClosed;
        private final int limit;

        public RowIterator(Iterator<N1qlQueryRow> rowIterator, int limit) {
            this.limit = limit;
            this.rows = Iterators.filter(rowIterator,
                row -> null != row && null != row.value().getString(CouchbaseColumn.ID));
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            ensureOpen();

            return new RecordIterator<Entry>() {
                private final Iterator<Entry> columns =
                    StaticArrayEntryList.ofBytes(
                        convertAndSortColumns(currentRow.value().getArray(CouchbaseColumn.COLUMNS), limit),
                        entryGetter).reuseIterator();

                @Override
                public boolean hasNext() {
                    ensureOpen();
                    return columns.hasNext();
                }

                @Override
                public Entry next() {
                    ensureOpen();
                    return columns.next();
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
            return getRowId(currentRow);
        }

        @Override
        public void close() {
            isClosed = true;
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
