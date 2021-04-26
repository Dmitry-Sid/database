package server.model.lock;

public interface Lock<T> {
    void lock(T value);

    void unlock(T value);
}
