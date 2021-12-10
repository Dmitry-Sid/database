package server.model;

import server.model.lock.Lock;

public class ChainedLock<T> implements Chained<T> {
    protected final Lock<T> lock;
    protected boolean closed = true;
    protected T currentValue;

    public ChainedLock(Lock<T> lock) {
        this.lock = lock;
    }

    @Override
    public void init(T value) {
        close();
        try {
            lock.lock(value);
            currentValue = value;
            initOthers(value);
            closed = false;
        } catch (Throwable e) {
            close();
            throw new RuntimeException(e);
        }
    }

    /**
     * Для переопределения в наследниках
     * @param value
     */
    protected void initOthers(T value) {
    }

    @Override
    public T getValue() {
        return currentValue;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        if (currentValue != null) {
            lock.unlock(currentValue);
            currentValue = null;
        }
    }
}
