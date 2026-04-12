package org.junify.db.core.event;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class EventBus {

    public enum EventType {
        BEFORE_INSERT, AFTER_INSERT,
        BEFORE_UPDATE, AFTER_UPDATE,
        BEFORE_DELETE, AFTER_DELETE,
        BEFORE_COMMIT, AFTER_COMMIT,
        BEFORE_ROLLBACK, AFTER_ROLLBACK,
        COLLECTION_CREATED, BUCKET_CREATED
    }

    public record Event(EventType type, String collection, Object data) {
    }

    private final List<Consumer<Event>> listeners = new CopyOnWriteArrayList<>();

    public void on(EventType type, Consumer<Event> handler) {
        listeners.add(event -> {
            if (event.type() == type) {
                handler.accept(event);
            }
        });
    }

    public void emit(EventType type, String collection, Object data) {
        var event = new Event(type, collection, data);
        for (var listener : listeners) {
            try {
                listener.accept(event);
            } catch (Exception ignored) {
            }
        }
    }

    public void emit(EventType type, String collection) {
        emit(type, collection, null);
    }

    public int listenerCount() {
        return listeners.size();
    }

    public void clear() {
        listeners.clear();
    }
}
