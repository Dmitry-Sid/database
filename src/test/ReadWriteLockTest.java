import org.junit.Test;
import sample.model.lock.ReadWriteLock;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static org.junit.Assert.assertTrue;

public class ReadWriteLockTest {

    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @Test
    public void fullTest() throws InterruptedException, ExecutionException {
        {
            {
                final ReadWriteLock<Integer> lock = new ReadWriteLock<>();
                lock.readLock(1);
                System.out.println("value : " + 1 + ", threadName : " + Thread.currentThread().getName() + " acquired lock");
                final Future<Long> future1 = createFuture(executorService, lock::readLock, lock::readUnlock, 1, null);
                final Future<Long> future2 = createFuture(executorService, lock::readLock, lock::readUnlock, 2, null);
                sleep(1);
                lock.readUnlock(1);
                System.out.println("value : " + 1 + ", threadName : " + Thread.currentThread().getName() + " released lock");
                future1.get();
                future2.get();
                assertTrue(future1.get() < 2000 && future2.get() < 2000);
            }
            System.out.println();
            {
                final ReadWriteLock<Integer> lock = new ReadWriteLock<>();
                lock.readLock(1);
                System.out.println("value : " + 1 + ", threadName : " + Thread.currentThread().getName() + " acquired lock");
                final Future<Long> future1 = createFuture(executorService, lock::writeLock, lock::writeUnlock, 1, null);
                final Future<Long> future2 = createFuture(executorService, lock::writeLock, lock::writeUnlock, 2, null);
                sleep(1);
                lock.readUnlock(1);
                System.out.println("value : " + 1 + ", threadName : " + Thread.currentThread().getName() + " released lock");
                future1.get();
                future2.get();
                assertTrue(future1.get() > 2000 && future1.get() < 3000);
                assertTrue(future2.get() < 2000);
            }
            System.out.println();
            {
                final ReadWriteLock<Integer> lock = new ReadWriteLock<>();
                lock.writeLock(1);
                System.out.println("value : " + 1 + ", threadName : " + Thread.currentThread().getName() + " acquired lock");
                final Future<Long> future1 = createFuture(executorService, lock::readLock, lock::readUnlock, 1, null);
                final Future<Long> future2 = createFuture(executorService, lock::readLock, lock::readUnlock, 2, null);
                sleep(1);
                lock.writeUnlock(1);
                System.out.println("value : " + 1 + ", threadName : " + Thread.currentThread().getName() + " released lock");
                future1.get();
                future2.get();
                assertTrue((future1.get() > 2000 && future1.get() < 3000) && (future2.get() > 1000 && future2.get() < 2000));
            }
            System.out.println();
            {
                final ReadWriteLock<Integer> lock = new ReadWriteLock<>();
                lock.writeLock(1);
                System.out.println("value : " + 1 + ", threadName : " + Thread.currentThread().getName() + " acquired lock");
                final Future<Long> future1 = createFuture(executorService, lock::writeLock, lock::writeUnlock, 1, null);
                final Future<Long> future2 = createFuture(executorService, lock::writeLock, lock::writeUnlock, 2, null);
                sleep(1);
                lock.writeUnlock(1);
                System.out.println("value : " + 1 + ", threadName : " + Thread.currentThread().getName() + " released lock");
                future1.get();
                future2.get();
                assertTrue((future1.get() > 2000 && future1.get() < 3000) && (future2.get() > 1000 && future2.get() < 2000));
            }
        }
    }

    @Test
    public void doubleLockTest() throws InterruptedException, ExecutionException {
        {
            final ReadWriteLock<Integer> lock = new ReadWriteLock<>();
            final long beginMain = System.currentTimeMillis();
            lock.readLock(1);
            lock.readLock(1);
            final Future<Long> future = createFuture(executorService, lock::readLock, lock::readUnlock, 1, null);
            sleep(1);
            lock.readUnlock(1);
            lock.readUnlock(1);
            final long time = System.currentTimeMillis() - beginMain;
            assertTrue(time > 1000 && time < 2000);
            future.get();
            assertTrue(future.get() > 1000 && future.get() < 2000);
        }
        System.out.println();
        {
            final ReadWriteLock<Integer> lock = new ReadWriteLock<>();
            final long beginMain = System.currentTimeMillis();
            lock.readLock(1);
            lock.writeLock(1);
            final Future<Long> future = createFuture(executorService, lock::readLock, lock::readUnlock, 1, null);
            sleep(1);
            lock.writeUnlock(1);
            lock.readUnlock(1);
            final long time = System.currentTimeMillis() - beginMain;
            assertTrue(time > 1000 && time < 2000);
            future.get();
            assertTrue(future.get() >= time + 1000);
        }
    }

    private <T> Future<Long> createFuture(ExecutorService executorService, Consumer<T> lockConsumer, Consumer<T> unlockConsumer, T value, Exception exc) {
        return executorService.submit(() -> {
            final long begin = System.currentTimeMillis();
            lockConsumer.accept(value);
            try {
                System.out.println("value : " + value + ", threadName : " + Thread.currentThread().getName() + " acquired lock");
                sleep(value);
                if (exc != null) {
                    throw exc;
                }
                return System.currentTimeMillis() - begin;
            } finally {
                unlockConsumer.accept(value);
                System.out.println("value : " + value + ", threadName : " + Thread.currentThread().getName() + " released lock");
            }
        });
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
