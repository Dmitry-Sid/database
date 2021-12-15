package server.model.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Этот лок предназначен для пропуска потоков с захваченным значением
 *
 * @param <T>
 */
public class SingleLock<T> implements Lock<T> {
    private static final Logger log = LoggerFactory.getLogger(SingleLock.class);
    private T currentValue;
    private int count = 0;

    @Override
    public synchronized void lock(T value) {
        try {
            if (value == null) {
                throw new IllegalArgumentException("value cannot be null");
            }
            while (currentValue != null && !currentValue.equals(value)) {
                try {
                    wait(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            currentValue = value;
            count++;
        } catch (Exception e) {
            log.error("error while locking value " + value, e);
            throw e;
        }
    }

    @Override
    public synchronized void unlock(T value) {
        try {
            if (value == null) {
                throw new IllegalArgumentException("value cannot be null");
            }
            if (!value.equals(currentValue) || count < 1) {
                throw new IllegalStateException("try to unlock not acquired value : " + value + ", thread : " + Thread.currentThread());
            }
            count--;
            if (count <= 0) {
                currentValue = null;
                count = 0;
            }
            notifyAll();
        } catch (Exception e) {
            log.error("error while unlocking value " + value, e);
            throw e;
        }
    }
}
