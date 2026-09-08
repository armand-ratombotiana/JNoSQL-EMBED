package org.junify.db.spring.boot;

import org.junify.db.JunifyDB;
import org.junify.db.nosql.column.ColumnFamily;
import org.junify.db.nosql.document.DocumentCollection;
import org.junify.db.nosql.kv.KeyValueBucket;

public class JunifyDBTemplate {

    private final JunifyDB db;

    public JunifyDBTemplate(JunifyDB db) {
        this.db = db;
    }

    public JunifyDB database() {
        return db;
    }

    public DocumentCollection documents(String collection) {
        return db.documentCollection(collection);
    }

    public KeyValueBucket keyValues(String bucket) {
        return db.keyValueBucket(bucket);
    }

    public ColumnFamily columns(String family) {
        return db.columnFamily(family);
    }
}
