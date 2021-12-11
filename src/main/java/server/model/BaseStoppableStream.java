package server.model;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class BaseStoppableStream<T> implements StoppableStream<T> {
    protected final AtomicBoolean stopChecker = new AtomicBoolean(false);
    protected final List<Runnable> onStreamEnd = new ArrayList<>();

    @Override
    public void addOnStreamEnd(Runnable action) {
        onStreamEnd.add(action);
    }

    @Override
    public void stop() {
        stopChecker.set(true);
    }
}
