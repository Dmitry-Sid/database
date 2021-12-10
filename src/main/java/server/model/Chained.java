package server.model;

import java.io.Closeable;

public interface Chained<T> extends Closeable {
    void init(T value);

    T getValue();

    boolean isClosed();
}
