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

import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;

/**
 * This class overrides and adds nothing compared with
 * {@link org.janusgraph.diskstorage.locking.consistentkey.ExpectedValueCheckingTransaction}; however, it creates a transaction type specific
 * to Couchbase, which lets us check for user errors like passing a Cassandra
 * transaction into a Couchbase method.
 *
 */
public class CouchbaseTransaction extends AbstractStoreTransaction {

    public CouchbaseTransaction(final BaseTransactionConfig config) {
        super(config);
    }
}
