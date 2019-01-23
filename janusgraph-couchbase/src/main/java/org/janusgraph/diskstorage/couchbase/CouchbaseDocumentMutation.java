package org.janusgraph.diskstorage.couchbase;

import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;

public class CouchbaseDocumentMutation {
    private String table;
    private String documentId;
    private KCVMutation mutation;

    public CouchbaseDocumentMutation(String table, String documentId, KCVMutation mutation) {
        this.table = table;
        this.documentId = documentId;
        this.mutation = mutation;
    }

    public String getTable() {
        return table;
    }

    public String getDocumentId() {
        return documentId;
    }

    public KCVMutation getMutation() {
        return mutation;
    }
}
