package org.junify.db.storage.spi;

import java.io.*;
import java.nio.file.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BiConsumer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

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
        this.writer = Executors.newSingleThreadExecutor(r -> {
            var t = new Thread(r, "WAL-Writer");
            t.setDaemon(true);
            return t;
        });
        this.archiver = Executors.newSingleThreadExecutor(r -> {
            var t = new Thread(r, "WAL-Archiver");
            t.setDaemon(true);
            return t;
        });
        initWriter();
        recoverIfNeeded();
    }

    public void setRecoveryCallback(BiConsumer<String, LogEntry> callback) {
        this.recoveryCallback = callback;
    }

    private void initWriter() throws IOException {
        var fileWriter = new FileWriter(walFile.toFile(), true);
        logWriter = new BufferedWriter(fileWriter);
    }

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
            
            if (shouldRotate()) {
                rotateWalFile();
            }
        } catch (IOException e) {
            System.err.println("WAL write failed: " + e.getMessage());
        }
    }

    private boolean shouldRotate() {
        try {
            return Files.size(walFile) > maxFileSizeKB * 1024;
        } catch (IOException e) {
            return false;
        }
    }

    private void rotateWalFile() throws IOException {
        logWriter.close();
        var timestamp = System.currentTimeMillis();
        var archivedFile = archiveDir.resolve("wal-" + timestamp + ".log.gz");
        
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

    public void close() throws IOException {
        closed.set(true);
        try {
            checkpoint();
        } catch (IOException e) {
            System.err.println("WAL checkpoint failed: " + e.getMessage());
        }
        writer.shutdown();
        archiver.shutdown();
        try {
            writer.awaitTermination(5, TimeUnit.SECONDS);
            archiver.awaitTermination(5, TimeUnit.SECONDS);
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
