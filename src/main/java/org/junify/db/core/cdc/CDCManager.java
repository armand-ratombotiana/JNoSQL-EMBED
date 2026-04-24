package org.junify.db.core.cdc;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class CDCManager {

    private final CDCProcessor processor;
    private final Map<String, FileCDCConnector> fileConnectors;
    private final Map<String, KafkaCDCConnector> kafkaConnectors;

    public CDCManager() {
        this.processor = new CDCProcessor();
        this.fileConnectors = new ConcurrentHashMap<>();
        this.kafkaConnectors = new ConcurrentHashMap<>();
    }

    public CDCProcessor processor() {
        return processor;
    }

    public void recordInsert(String collection, String key, String value) {
        processor.onEvent(CDCEvent.insert(collection, key, value));
    }

    public void recordUpdate(String collection, String key, String oldValue, String newValue) {
        processor.onEvent(CDCEvent.update(collection, key, oldValue, newValue));
    }

    public void recordDelete(String collection, String key, String oldValue) {
        processor.onEvent(CDCEvent.delete(collection, key, oldValue));
    }

    public FileCDCConnector addFileConnector(String name, java.nio.file.Path outputDir) {
        var connector = new FileCDCConnector(processor, outputDir, name);
        connector.start();
        fileConnectors.put(name, connector);
        return connector;
    }

    public void removeFileConnector(String name) {
        var connector = fileConnectors.remove(name);
        if (connector != null) {
            connector.close();
        }
    }

    public KafkaCDCConnector addKafkaConnector(String name, String bootstrapServers, String topic) {
        var connector = new KafkaCDCConnector(processor, bootstrapServers, topic);
        connector.start();
        kafkaConnectors.put(name, connector);
        return connector;
    }

    public void removeKafkaConnector(String name) {
        var connector = kafkaConnectors.remove(name);
        if (connector != null) {
            connector.close();
        }
    }

    public Map<String, Object> getStatus() {
        return Map.of(
            "enabled", processor.isEnabled(),
            "eventsInLog", processor.getEventLog().size(),
            "subscribers", processor.getEventLog().size(),
            "fileConnectors", fileConnectors.keySet(),
            "kafkaConnectors", kafkaConnectors.keySet()
        );
    }

    public void close() {
        processor.disable();
        for (var connector : fileConnectors.values()) {
            connector.close();
        }
        fileConnectors.clear();
        
        for (var connector : kafkaConnectors.values()) {
            connector.close();
        }
        kafkaConnectors.clear();
        
        processor.clear();
    }
}