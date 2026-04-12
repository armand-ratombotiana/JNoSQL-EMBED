package org.junify.db.reactive;

import org.junify.db.JunifyDB;
import org.junify.db.config.JunifyDBConfig;
import org.junify.db.kv.KeyValueBucket;
import org.junify.db.transaction.Transaction;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ReactiveJNoSQL {

    private final JunifyDB delegate;
    private final ExecutorService executor;

    public ReactiveJNoSQL(JunifyDB delegate) {
        this.delegate = delegate;
        this.executor = Executors.newCachedThreadPool();
    }

    public ReactiveJNoSQL(JunifyDB delegate, ExecutorService executor) {
        this.delegate = delegate;
        this.executor = executor;
    }

    public ReactiveDocumentCollection documentCollection(String name) {
        return new ReactiveDocumentCollection(delegate.documentCollection(name), executor);
    }

    public ReactiveKeyValueBucket keyValueBucket(String name) {
        return new ReactiveKeyValueBucket(delegate.keyValueBucket(name), executor);
    }

    public CompletableFuture<Transaction> beginTransaction() {
        return CompletableFuture.supplyAsync(delegate::beginTransaction, executor);
    }

    public CompletableFuture<Void> close() {
        return CompletableFuture.runAsync(() -> {
            delegate.close();
            executor.shutdown();
        }, executor);
    }

    public JunifyDB delegate() {
        return delegate;
    }

    public ExecutorService executor() {
        return executor;
    }

    public static ReactiveJNoSQL wrap(JunifyDB JunifyDB) {
        return new ReactiveJNoSQL(JunifyDB);
    }
}
