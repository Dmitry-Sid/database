import org.junit.Test;
import server.model.lock.Lock;
import server.model.lock.ReadWriteLock;
import server.model.lock.ReadWriteLockImpl2;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertTrue;

public class ReadWriteLockTest2 {

    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @Test
    public void fullTest() throws InterruptedException, ExecutionException {
        {
            {
                final ReadWriteLock<Integer> lock = new ReadWriteLockImpl2<>();
                lock.readLock().lock(1);
                System.out.println("value : " + 1 + ", threadName : " + Thread.currentThread().getName() + " acquired lock");
                final Future<Long> future1 = TestUtils.createFuture(executorService, lock.readLock(), 1, null);
                final Future<Long> future2 = TestUtils.createFuture(executorService, lock.readLock(), 2, null);
                final Future<Long> future3 = TestUtils.createFuture(executorService, lock.readLock(), 1, null);
                sleep(1);
                lock.readLock().unlock(1);
                System.out.println("value : " + 1 + ", threadName : " + Thread.currentThread().getName() + " released lock");
                future1.get();
                future2.get();
                future3.get();
                assertTrue(future1.get() < 2000 && future2.get() < 2000 && future3.get() < 2000);
            }
            System.out.println();
            {
                final ReadWriteLock<Integer> lock = new ReadWriteLockImpl2<>();
                lock.readLock().lock(1);
                System.out.println("value : " + 1 + ", threadName : " + Thread.currentThread().getName() + " acquired lock");
                final Future<Long> future1 = TestUtils.createFuture(executorService, lock.writeLock(), 1, null);
                final Future<Long> future2 = TestUtils.createFuture(executorService, lock.writeLock(), 2, null);
                final Future<Long> future3 = TestUtils.createFuture(executorService, lock.writeLock(), 1, null);
                sleep(1);
                lock.readLock().unlock(1);
                System.out.println("value : " + 1 + ", threadName : " + Thread.currentThread().getName() + " released lock");
                future1.get();
                future2.get();
                future3.get();
                assertTrue(future1.get() + future3.get() > 5000);
                assertTrue(future2.get() < 2000);
            }
            System.out.println();
            {
                final ReadWriteLock<Integer> lock = new ReadWriteLockImpl2<>();
                lock.writeLock().lock(1);
                System.out.println("value : " + 1 + ", threadName : " + Thread.currentThread().getName() + " acquired lock");
                final Future<Long> future1 = TestUtils.createFuture(executorService, lock.readLock(), 1, null);
                final Future<Long> future2 = TestUtils.createFuture(executorService, lock.readLock(), 2, null);
                final Future<Long> future3 = TestUtils.createFuture(executorService, lock.readLock(), 1, null);
                sleep(1);
                lock.writeLock().unlock(1);
                System.out.println("value : " + 1 + ", threadName : " + Thread.currentThread().getName() + " released lock");
                future1.get();
                future2.get();
                future3.get();
                assertTrue(future1.get() + future3.get() > 4000);
                assertTrue(future2.get() < 2000);
            }
            System.out.println();
            {
                final ReadWriteLock<Integer> lock = new ReadWriteLockImpl2<>();
                lock.writeLock().lock(1);
                System.out.println("value : " + 1 + ", threadName : " + Thread.currentThread().getName() + " acquired lock");
                final Future<Long> future1 = TestUtils.createFuture(executorService, lock.writeLock(), 1, null);
                final Future<Long> future2 = TestUtils.createFuture(executorService, lock.writeLock(), 2, null);
                final Future<Long> future3 = TestUtils.createFuture(executorService, lock.writeLock(), 1, null);
                sleep(1);
                lock.writeLock().unlock(1);
                System.out.println("value : " + 1 + ", threadName : " + Thread.currentThread().getName() + " released lock");
                future1.get();
                future2.get();
                future3.get();
                assertTrue(future1.get() + future3.get() > 5000);
                assertTrue(future2.get() < 2000);
            }
        }
    }

    private <T> void sleep(T value) throws InterruptedException {
        int counter = 0;
        final long beginInLock = System.currentTimeMillis();
        while (System.currentTimeMillis() - beginInLock < 1000) {
            counter++;
            Thread.sleep(100);
        }
        System.out.println("value : " + value + ", threadName : " + Thread.currentThread().getName() + " counter " + counter);
    }

}
