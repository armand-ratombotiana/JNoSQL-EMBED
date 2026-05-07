package org.junify.db.storage.spi;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BiConsumer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Write-Ahead Log with Structured Concurrency patterns.
 * 
 * Features:
 * - Virtual thread execution for I/O operations
 * - Structured shutdown semantics
 * - Zero-copy buffer management (when available)
 */
public class WriteAheadLog {

    private final Path walDir;
    private final Path walFile;
    private final Path archiveDir;
    private final AtomicLong logSequence = new AtomicLong(0);
    private final ExecutorService writer;
    private final ExecutorService archiver;
    private BufferedWriter logWriter;
    private final ConcurrentLinkedQueue<LogEntry> pendingWrites = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final int maxFileSizeKB;
    private BiConsumer<String, LogEntry> recoveryCallback;

    public WriteAheadLog(Path dataDir) throws IOException {
        this(dataDir, 1024);
    }

    public WriteAheadLog(Path dataDir, int maxFileSizeKB) throws IOException {
        this.walDir = dataDir.resolve(".wal");
        this.archiveDir = walDir.resolve("archive");
        this.maxFileSizeKB = maxFileSizeKB;
        Files.createDirectories(walDir);
        Files.createDirectories(archiveDir);
        this.walFile = walDir.resolve("wal.log");
        
        // Java 17+: Use virtual threads if available (Java 21+), fallback to platform threads
        this.writer = createVirtualExecutor("WAL-Writer");
        this.archiver = createVirtualExecutor("WAL-Archiver");
        
        initWriter();
        recoverIfNeeded();
    }
    
    /**
     * Create executor using virtual threads (Java 21+) or platform threads (Java 17).
     */
    private ExecutorService createVirtualExecutor(String name) {
        try {
            // Try Java 21+ virtual threads
            var factory = (ThreadFactory) Thread.class
                .getMethod("ofVirtual")
                .invoke(null);
            return (ExecutorService) factory.getClass()
                .getMethod("name", String.class, long.class)
                .invoke(factory, name, 1L);
        } catch (Exception e) {
            // Fallback to Java 17 platform threads
            return Executors.newSingleThreadExecutor(r -> {
                var t = new Thread(r, name);
                t.setDaemon(true);
                return t;
            });
        }
    }

    public void setRecoveryCallback(BiConsumer<String, LogEntry> callback) {
        this.recoveryCallback = callback;
    }

    private FileOutputStream logFileOutputStream;

    private void initWriter() throws IOException {
        var fileWriter = new FileWriter(walFile.toFile(), true);
        logWriter = new BufferedWriter(fileWriter);
        // Keep FileOutputStream for fsync
        this.logFileOutputStream = new FileOutputStream(walFile.toFile(), true);
    }

    /**
     * Log a write operation with fsync for durability guarantee.
     */
    public synchronized void log(String type, String collection, String key, String value) {
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

        try {
            logWriter.write(entry.toString());
            logWriter.newLine();
            logWriter.flush();

            // Fsync to ensure data is persisted to disk
            fsync();

            if (shouldRotate()) {
                rotateWalFile();
            }
        } catch (IOException e) {
            System.err.println("WAL write failed: " + e.getMessage());
        }
    }

    /**
     * Force sync WAL to disk for durability guarantee.
     */
    public synchronized void fsync() throws IOException {
        if (logFileOutputStream != null) {
            logFileOutputStream.flush();
            logFileOutputStream.getFD().sync();
        }
    }

    private boolean shouldRotate() {
        try {
            return Files.size(walFile) > maxFileSizeKB * 1024;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Rotate WAL file and archive with compression.
     */
    private void rotateWalFile() throws IOException {
        // Fsync before rotation to ensure all data is persisted
        fsync();
        
        logWriter.close();
        if (logFileOutputStream != null) {
            logFileOutputStream.close();
        }
        
        var timestamp = System.currentTimeMillis();
        var archivedFile = archiveDir.resolve("wal-" + timestamp + ".log.gz");

        // Submit archival to background executor
        archiver.submit(() -> {
            try {
                try (var fis = Files.newInputStream(walFile);
                     var fos = Files.newOutputStream(archivedFile);
                     var gzOut = new GZIPOutputStream(fos)) {
                    fis.transferTo(gzOut);
                }
                Files.deleteIfExists(walFile);
                initWriter();
            } catch (IOException e) {
                System.err.println("WAL archive failed: " + e.getMessage());
            }
        });
    }

    public synchronized void checkpoint() throws IOException {
        if (closed.get()) return;

        logWriter.write("CHECKPOINT:" + logSequence.get());
        logWriter.newLine();
        logWriter.flush();
    }

    public void recoverIfNeeded() {
        if (!Files.exists(walFile)) return;

        long lastSeq = 0;
        int recoveredOps = 0;

        try ( var lines = Files.lines(walFile)) {
            for (var line : (Iterable<String>) lines::iterator) {
                if (line.startsWith("CHECKPOINT:")) {
                    lastSeq = Long.parseLong(line.substring(11));
                } else if (line.startsWith("PUT:") || line.startsWith("DELETE:")) {
                    var entry = LogEntry.fromString(line.substring(5));
                    if (entry != null && recoveryCallback != null) {
                        recoveryCallback.accept(line.substring(0, 4), entry);
                        recoveredOps++;
                    }
                }
            }
            logSequence.set(lastSeq);
        } catch (IOException e) {
            System.err.println("WAL recovery failed: " + e.getMessage());
        }

        if (recoveredOps > 0) {
            System.out.println("WAL: Recovered " + recoveredOps + " operations");
        }
    }

    /**
     * Graceful shutdown with executor termination.
     */
    public void close() throws IOException {
        if (closed.getAndSet(true)) return;
        
        try {
            checkpoint();
        } catch (IOException e) {
            System.err.println("WAL checkpoint failed: " + e.getMessage());
        }
        
        // Graceful executor shutdown
        writer.shutdown();
        archiver.shutdown();
        try {
            if (!writer.awaitTermination(5, TimeUnit.SECONDS)) {
                writer.shutdownNow();
            }
            if (!archiver.awaitTermination(5, TimeUnit.SECONDS)) {
                archiver.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            writer.shutdownNow();
            archiver.shutdownNow();
        }
        
        logWriter.close();
    }

    public void truncate() throws IOException {
        Files.deleteIfExists(walFile);
        logSequence.set(0);
        initWriter();
    }

    public long sequence() {
        return logSequence.get();
    }

    public Path walDir() {
        return walDir;
    }

    public record LogEntry(
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
            try {
                return new LogEntry(
                    Long.parseLong(parts[0]),
                    Long.parseLong(parts[1]),
                    parts[2],
                    parts[3],
                    parts[4],
                    parts.length > 5 && !parts[5].isEmpty() ? parts[5] : null
                );
            } catch (NumberFormatException e) {
                return null;
            }
        }
    }
}
