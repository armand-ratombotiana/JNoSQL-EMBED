package org.jnosql.embed.reactive;

import org.jnosql.embed.JNoSQL;
import org.jnosql.embed.config.JNoSQLConfig;
import org.jnosql.embed.kv.KeyValueBucket;
import org.jnosql.embed.transaction.Transaction;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ReactiveJNoSQL {

    private final JNoSQL delegate;
    private final ExecutorService executor;

    public ReactiveJNoSQL(JNoSQL delegate) {
        this.delegate = delegate;
        this.executor = Executors.newCachedThreadPool();
    }

    public ReactiveJNoSQL(JNoSQL delegate, ExecutorService executor) {
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

    public JNoSQL delegate() {
        return delegate;
    }

    public ExecutorService executor() {
        return executor;
    }

    public static ReactiveJNoSQL wrap(JNoSQL jnosql) {
        return new ReactiveJNoSQL(jnosql);
    }
}
