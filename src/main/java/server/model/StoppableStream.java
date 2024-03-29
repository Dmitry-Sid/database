package server.model;

import java.util.function.Consumer;

public interface StoppableStream<T> {
    void forEach(Consumer<T> consumer);

    void addOnStreamEnd(Runnable action);

    void stop();
}
