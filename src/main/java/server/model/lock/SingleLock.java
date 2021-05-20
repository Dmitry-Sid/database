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
        if (value == null) {
            return;
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
    }

    @Override
    public synchronized void unlock(T value) {
        if (value == null) {
            return;
        }
        if (!value.equals(currentValue) || count < 1) {
            log.warn("try to unlock not acquired value : " + value + ", thread : " + Thread.currentThread());
            return;
        }
        count--;
        if (count <= 0) {
            currentValue = null;
            count = 0;
        }
        notifyAll();
    }
}
