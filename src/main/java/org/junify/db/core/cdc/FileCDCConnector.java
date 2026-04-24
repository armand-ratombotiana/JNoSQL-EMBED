package org.junify.db.core.cdc;

import java.io.*;
import java.nio.file.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class FileCDCConnector implements AutoCloseable {

    private final Path outputDir;
    private final CDCProcessor processor;
    private final ExecutorService writer;
    private final BlockingQueue<CDCEvent> queue;
    private final AtomicBoolean running;
    private final String connectorId;

    public FileCDCConnector(CDCProcessor processor, Path outputDir) {
        this(processor, outputDir, "cdc-" + System.currentTimeMillis());
    }

    public FileCDCConnector(CDCProcessor processor, Path outputDir, String connectorId) {
        this.processor = processor;
        this.outputDir = outputDir;
        this.connectorId = connectorId;
        this.queue = new LinkedBlockingQueue<>(10000);
        this.running = new AtomicBoolean(false);
        this.writer = Executors.newSingleThreadExecutor(r -> {
            var t = new Thread(r, "CDC-FileWriter");
            t.setDaemon(true);
            return t;
        });

        try {
            Files.createDirectories(outputDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create CDC output directory", e);
        }
    }

    public void start() {
        if (running.getAndSet(true)) return;
        writer.submit(this::writeLoop);
    }

    public void stop() {
        running.set(false);
    }

    private void writeLoop() {
        Path currentFile = outputDir.resolve(connectorId + ".jsonl");
        BufferedWriter writer = null;

        while (running.get()) {
            try {
                CDCEvent event = queue.poll(1, TimeUnit.SECONDS);
                if (event == null) continue;

                if (writer == null) {
                    writer = Files.newBufferedWriter(currentFile, 
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                }

                writer.write(event.toJson());
                writer.newLine();
                writer.flush();

                if (Files.size(currentFile) > 100 * 1024 * 1024) {
                    writer.close();
                    String timestamp = String.valueOf(System.currentTimeMillis());
                    Path archivedFile = outputDir.resolve(connectorId + "-" + timestamp + ".jsonl");
                    Files.move(currentFile, archivedFile);
                    writer = Files.newBufferedWriter(currentFile,
                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (IOException e) {
                System.err.println("CDC file write error: " + e.getMessage());
            }
        }

        try {
            if (writer != null) writer.close();
        } catch (IOException ignored) {}
    }

    public boolean offer(CDCEvent event) {
        return queue.offer(event);
    }

    public int queueSize() {
        return queue.size();
    }

    @Override
    public void close() {
        stop();
        writer.shutdown();
        try {
            writer.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}