import org.junit.Test;
import server.model.lock.Lock;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

public class LockTest {

    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @Test
    public void fullTest() throws InterruptedException, ExecutionException {
        {
            final Lock<Integer> lock = new Lock<>();
            lock.lock(1);
            final Future<Long> future1 = createFuture(executorService, lock, 1, null);
            final Future<Long> future2 = createFuture(executorService, lock, 2, null);
            final Future<Long> future3 = createFuture(executorService, lock, 1, null);
            Thread.sleep(1000);
            lock.unlock(1);
            assertTrue(future1.get() + future3.get() > 5000);
            assertTrue(future2.get() < 2000);

            lock.lock(3);
            final Future<Long> future4 = createFuture(executorService, lock, 3, new RuntimeException("test exception"));
            final Future<Long> future5 = createFuture(executorService, lock, 3, null);
            Thread.sleep(1000);
            lock.unlock(3);
            String exception = null;
            try {
                future4.get();
            } catch (Exception exc) {
                exception = exc.getMessage();
            }
            assertEquals("java.lang.RuntimeException: test exception", exception);
            assertTrue(future5.get() > 2000);
        }
        {
            final Lock<String> lock = new Lock<>();
            lock.lock("test1");
            final Future<Long> future1 = createFuture(executorService, lock, "test1", null);
            final Future<Long> future2 = createFuture(executorService, lock, "test2", null);
            final Future<Long> future3 = createFuture(executorService, lock, "test1", null);
            Thread.sleep(1000);
            lock.unlock("test1");
            assertTrue(future1.get() + future3.get() > 5000);
            assertTrue(future2.get() < 2000);
        }
    }

    @Test
    public void doubleLockTest() throws InterruptedException, ExecutionException {
        final Lock<Integer> lock = new Lock<>();
        lock.lock(1);
        lock.lock(1);
        final Future<Long> future = executorService.submit(() -> {
            final long begin = System.currentTimeMillis();
            lock.lock(1);
            lock.lock(1);
            try {
                Thread.sleep(1000);
                return System.currentTimeMillis() - begin;
            } finally {
                lock.unlock(1);
            }
        });
        Thread.sleep(1000);
        lock.unlock(1);
        assertFalse(future.isDone());
        Thread.sleep(1000);
        lock.unlock(1);
        assertTrue(future.get() > 3000 && future.get() < 4000);
    }

    private <T> Future<Long> createFuture(ExecutorService executorService, Lock<T> lock, T value, Exception exc) {
        return executorService.submit(() -> {
            final long begin = System.currentTimeMillis();
            lock.lock(value);
            try {
                Thread.sleep(1000);
                if (exc != null) {
                    throw exc;
                }
                return System.currentTimeMillis() - begin;
            } finally {
                lock.unlock(value);
            }
        });
    }

}
