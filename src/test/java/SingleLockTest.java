import org.junit.Test;
import server.model.lock.Lock;
import server.model.lock.SingleLock;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

public class SingleLockTest {

    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @Test
    public void fullTest() throws InterruptedException, ExecutionException {
        {
            final Lock<Integer> lock = new SingleLock<>();
            lock.lock(1);
            final Future<Long> future1 = TestUtils.createFuture(executorService, lock, 2, null);
            final Future<Long> future2 = TestUtils.createFuture(executorService, lock, 1, null);
            final Future<Long> future3 = TestUtils.createFuture(executorService, lock, 2, null);
            Thread.sleep(1000);
            lock.unlock(1);
            assertTrue(future1.get() + future3.get() > 4000);
            assertTrue(future2.get() < 2000);

            lock.lock(3);
            final Future<Long> future4 = TestUtils.createFuture(executorService, lock, 3, new RuntimeException("test exception"));
            final Future<Long> future5 = TestUtils.createFuture(executorService, lock, 4, null);
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
            final Lock<String> lock = new SingleLock<>();
            lock.lock("test1");
            final Future<Long> future1 = TestUtils.createFuture(executorService, lock, "test2", null);
            final Future<Long> future2 = TestUtils.createFuture(executorService, lock, "test1", null);
            final Future<Long> future3 = TestUtils.createFuture(executorService, lock, "test2", null);
            Thread.sleep(1000);
            lock.unlock("test1");
            assertTrue(future1.get() + future3.get() > 4000);
            assertTrue(future2.get() < 2000);
        }
    }
}
