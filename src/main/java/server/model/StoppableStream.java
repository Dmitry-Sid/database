package server.model;

import java.util.function.Consumer;

public interface StoppableStream<T> {
    void forEach(Consumer<T> consumer);

    default void forEach(Consumer<T> consumer, Runnable onStreamEnd) {
        forEach(consumer);
    }

    void stop();
}
