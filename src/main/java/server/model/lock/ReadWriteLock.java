package server.model.lock;

public interface ReadWriteLock<T> {
    Lock<T> readLock();

    Lock<T> writeLock();
}
