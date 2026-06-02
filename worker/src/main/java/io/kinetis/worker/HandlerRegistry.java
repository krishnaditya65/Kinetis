package io.kinetis.worker;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Maps {@code job_type} → {@link JobHandler}. A worker advertises the types it can execute via this. */
public class HandlerRegistry {

    private final Map<String, JobHandler> handlers = new ConcurrentHashMap<>();

    public HandlerRegistry(List<JobHandler> handlers) {
        handlers.forEach(this::register);
    }

    public void register(JobHandler handler) {
        JobHandler prev = handlers.putIfAbsent(handler.type(), handler);
        if (prev != null) {
            throw new IllegalStateException("duplicate handler for type: " + handler.type());
        }
    }

    public Optional<JobHandler> get(String type) {
        return Optional.ofNullable(handlers.get(type));
    }
}
