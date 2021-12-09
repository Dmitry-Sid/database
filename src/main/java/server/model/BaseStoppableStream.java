package server.model;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class BaseStoppableStream<T> implements StoppableStream<T> {
    protected final AtomicBoolean stopChecker = new AtomicBoolean(false);

    @Override
    public void stop() {
        stopChecker.set(true);
    }
}
