package org.jnosql.embed.storage;

import java.io.*;
import java.nio.file.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class WriteAheadLog {

    private final Path walDir;
    private final Path walFile;
    private final AtomicLong logSequence = new AtomicLong(0);
    private final ExecutorService writer;
    private BufferedWriter logWriter;
    private final ConcurrentLinkedQueue<LogEntry> pendingWrites = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public WriteAheadLog(Path dataDir) throws IOException {
        this.walDir = dataDir.resolve(".wal");
        Files.createDirectories(walDir);
        this.walFile = walDir.resolve("wal.log");
        this.writer = Executors.newSingleThreadExecutor();
        initWriter();
        recoverIfNeeded();
    }

    private void initWriter() throws IOException {
        var fileWriter = new FileWriter(walFile.toFile(), true);
        logWriter = new BufferedWriter(fileWriter);
    }

    public void log(String type, String collection, String key, String value) {
        if (closed.get()) return;
        
        var entry = new LogEntry(
            logSequence.incrementAndGet(),
            System.currentTimeMillis(),
            type,
            collection,
            key,
            value
        );
        
        pendingWrites.offer(entry);
        writer.execute(() -> flushEntry(entry));
    }

    private synchronized void flushEntry(LogEntry entry) {
        try {
            logWriter.write(entry.toString());
            logWriter.newLine();
            logWriter.flush();
        } catch (IOException e) {
            System.err.println("WAL write failed: " + e.getMessage());
        }
    }

    public void checkpoint() throws IOException {
        if (closed.get()) return;
        
        synchronized (this) {
            logWriter.write("CHECKPOINT:" + logSequence.get());
            logWriter.newLine();
            logWriter.flush();
        }
    }

    private void recoverIfNeeded() {
        if (!Files.exists(walFile)) return;
        
        try (var lines = Files.lines(walFile)) {
            lines.forEach(line -> {
                if (line.startsWith("CHECKPOINT:")) {
                    var seq = Long.parseLong(line.substring(11));
                    logSequence.set(seq);
                }
            });
        } catch (IOException e) {
            System.err.println("WAL recovery failed: " + e.getMessage());
        }
    }

    public void close() throws IOException {
        closed.set(true);
        checkpoint();
        writer.shutdown();
        try {
            writer.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        logWriter.close();
    }

    public void truncate() throws IOException {
        Files.deleteIfExists(walFile);
        logSequence.set(0);
        initWriter();
    }

    private record LogEntry(
        long sequence,
        long timestamp,
        String type,
        String collection,
        String key,
        String value
    ) {
        @Override
        public String toString() {
            return sequence + "|" + timestamp + "|" + type + "|" + collection + "|" + key + "|" + (value != null ? value : "");
        }

        public static LogEntry fromString(String line) {
            var parts = line.split("\\|", 6);
            if (parts.length < 5) return null;
            return new LogEntry(
                Long.parseLong(parts[0]),
                Long.parseLong(parts[1]),
                parts[2],
                parts[3],
                parts[4],
                parts.length > 5 ? parts[5] : null
            );
        }
    }
}
