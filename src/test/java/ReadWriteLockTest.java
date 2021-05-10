import org.junit.Test;
import server.model.lock.ReadWriteLock;
import server.model.lock.ReadWriteLockImpl;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertTrue;

public class ReadWriteLockTest {

    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @Test
    public void fullTest() throws InterruptedException, ExecutionException {
        {
            {
                final ReadWriteLock<Integer> lock = new ReadWriteLockImpl<>();
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
                final ReadWriteLock<Integer> lock = new ReadWriteLockImpl<>();
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
                final ReadWriteLock<Integer> lock = new ReadWriteLockImpl<>();
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
                final ReadWriteLock<Integer> lock = new ReadWriteLockImpl<>();
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

    @Test
    public void doubleLockTest() throws InterruptedException, ExecutionException {
        {
            final ReadWriteLock<Integer> lock = new ReadWriteLockImpl<>();
            final long beginMain = System.currentTimeMillis();
            lock.readLock().lock(1);
            lock.readLock().lock(1);
            final Future<Long> future = TestUtils.createFuture(executorService, lock.readLock(), 1, null);
            sleep(1);
            lock.readLock().unlock(1);
            lock.readLock().unlock(1);
            final long time = System.currentTimeMillis() - beginMain;
            assertTrue(time > 1000 && time < 2000);
            future.get();
            assertTrue(future.get() > 1000 && future.get() < 2000);
        }
        System.out.println();
        {
            final ReadWriteLock<Integer> lock = new ReadWriteLockImpl<>();
            final long beginMain = System.currentTimeMillis();
            lock.readLock().lock(1);
            lock.writeLock().lock(1);
            final Future<Long> future = TestUtils.createFuture(executorService, lock.readLock(), 1, null);
            sleep(1);
            lock.writeLock().unlock(1);
            lock.readLock().unlock(1);
            final long time = System.currentTimeMillis() - beginMain;
            assertTrue(time > 1000 && time < 2000);
            future.get();
            assertTrue(future.get() >= time + 1000);
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
