import server.model.*;
import server.model.impl.DataCompressorImpl;
import server.model.impl.ObjectConverterImpl;
import server.model.impl.RowIdRepositoryImpl;
import server.model.lock.Lock;
import server.model.pojo.Row;
import server.model.pojo.RowAddress;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class TestUtils {
    public static Row generateRow(int id, int intValue) {
        return new Row(id, new HashMap<String, Comparable>() {{
            put("field" + intValue, intValue);
        }});
    }

    public static RowIdRepository prepareRowIdRepository(String filePath, DestroyService destroyService, int maxIdSize, int compressSize, ObjectConverter objectConverter) {
        return new RowIdRepositoryImpl(filePath, true, objectConverter, destroyService, maxIdSize, compressSize);
    }

    public static ModelService mockModelService() {
        final ModelService modelService = mock(ModelService.class);
        when(modelService.contains(anyString())).thenAnswer(invocation -> {
            final String field = (String) invocation.getArguments()[0];
            return "String".equalsIgnoreCase(field) || "int".equalsIgnoreCase(field) || "double".equalsIgnoreCase(field);
        });
        when(modelService.getValue(anyString(), anyString())).thenAnswer(invocation -> {
            final String field = (String) invocation.getArguments()[0];
            final String value = (String) invocation.getArguments()[1];
            if ("String".equalsIgnoreCase(field)) {
                return value;
            } else if ("int".equalsIgnoreCase(field)) {
                return Integer.parseInt(value);
            } else if ("double".equalsIgnoreCase(field)) {
                return Double.parseDouble(value);
            }
            throw new RuntimeException("unknown field " + field);
        });
        return modelService;
    }

    public static void createRowIdFiles(int lastId, int maxIdSize, int compressSize, String fileName, String filesIdPath, String filesRowPath, int rowAddressSize, Map<Integer, byte[]> rowMap) {
        final int maxRowSize = maxIdSize / compressSize;
        final Set<Integer> boundsBatch = prepareBoundsBatch(lastId, maxIdSize);
        final ObjectConverter objectConverter = new ObjectConverterImpl(new DataCompressorImpl());
        RowIdRepositoryImpl.CachedRowAddresses cachedRowAddresses = null;
        int id = 1;
        int lastRowFileNumber = 1;
        for (Integer value : boundsBatch) {
            final Map<Integer, RowAddress> rowAddressMap = new ConcurrentHashMap<>();
            int lastEndPosition = 0;
            RowAddress rowAddress = null;
            RowAddress rowAddressPrevious = null;
            final int max = Math.min(value * maxIdSize, lastId);
            for (int i = (value - 1) * maxIdSize + 1; i <= max; i++) {
                final int rowFileNumber = getRowFileNumber(id, maxRowSize);
                if (lastRowFileNumber != rowFileNumber) {
                    lastRowFileNumber = rowFileNumber;
                    lastEndPosition = 0;
                    rowAddressPrevious = null;
                }
                rowAddress = new RowAddress(filesRowPath + getRowFileNumber(id, maxRowSize), id, lastEndPosition, rowMap == null ? rowAddressSize : rowMap.get(id).length);
                lastEndPosition += rowAddress.getSize();
                if (rowAddressPrevious != null) {
                    rowAddressPrevious.setNext(rowAddress.getId());
                    rowAddress.setPrevious(rowAddressPrevious.getId());
                }
                rowAddressPrevious = rowAddress;
                rowAddressMap.put(id, rowAddress);
                id++;
            }
            cachedRowAddresses = new RowIdRepositoryImpl.CachedRowAddresses(rowAddressMap, rowAddress);
            objectConverter.toFile(cachedRowAddresses, filesIdPath + value);
        }
        final RowIdRepositoryImpl.Variables variables = new RowIdRepositoryImpl.Variables(new AtomicInteger(lastId), boundsBatch, new ConcurrentHashMap<>());
        objectConverter.toFile(variables, fileName);
    }

    public static Set<Integer> prepareBoundsBatch(int lastId, int maxIdSize) {
        final Set<Integer> set = Collections.synchronizedSet(new LinkedHashSet<>());
        for (int i = 0; i <= lastId / maxIdSize; i++) {
            set.add(i + 1);
        }
        return set;
    }

    public static int getRowIdFileNumber(int id, int maxIdSize) {
        return 1 + (id - 1) / maxIdSize;
    }

    public static int getRowFileNumber(int id, int maxRowSize) {
        return 1 + (id - 1) / maxRowSize;
    }

    public static Map<Integer, byte[]> createRowMap(int lastId) {
        final ObjectConverter objectConverter = new ObjectConverterImpl(new DataCompressorImpl());
        final Map<Integer, byte[]> map = new HashMap<>();
        for (int i = 1; i <= lastId; i++) {
            map.put(i, objectConverter.toBytes(generateRow(i, i)));
        }
        return map;
    }

    public static void createRowFiles(Map<Integer, byte[]> map, String rowFileName, int maxRowSize) {
        try {
            String lastFileName = null;
            FileOutputStream fileOutputStream = null;
            for (Map.Entry<Integer, byte[]> entry : map.entrySet()) {
                final String fileName = rowFileName + getRowFileNumber(entry.getKey(), maxRowSize);
                if (!fileName.equals(lastFileName)) {
                    if (fileOutputStream != null) {
                        fileOutputStream.close();
                    }
                    fileOutputStream = new FileOutputStream(fileName);
                }

                lastFileName = fileName;
                fileOutputStream.write(entry.getValue());
            }
            if (fileOutputStream != null) {
                fileOutputStream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void doAndSleep(Destroyable destroyable, Runnable runnable) {
        doAndSleep(destroyable, runnable, 3000);
    }

    public static void doAndSleep(Destroyable destroyable, Runnable runnable, long sleepTime) {
        runnable.run();
        if (destroyable instanceof BaseDestroyable) {
            destroyable.stop();
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        destroyable.destroy();
    }

    public static void assertBytes(byte[] expected, byte[] actual) {
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i]);
        }
    }

    public static <T> void lock(Lock<T> lock, Set<T> set, T value) {
        lock.lock(value);
        set.add(value);
    }

    public static <T> void unlock(Lock<T> lock, Set<T> set, T value) {
        set.remove(value);
        lock.unlock(value);
    }

    public static class LockBean<T> {
        public final ExecutorService executorService;
        public final Set<T> lockedValues;

        public LockBean(ExecutorService executorService, Set<T> lockedValues) {
            this.executorService = executorService;
            this.lockedValues = lockedValues;
        }

        public Future<Long> run(Lock<T> lock, T value, String exception, boolean gottaWait) {
            return executorService.submit(() -> {
                final long begin = System.currentTimeMillis();
                synchronized (LockBean.this) {
                    if (gottaWait && !lockedValues.contains(value)) {
                        fail("lock failed");
                    }
                }
                lock.lock(value);
                synchronized (LockBean.this) {
                    if (gottaWait && lockedValues.contains(value)) {
                        fail("lock failed");
                    }
                }
                synchronized (LockBean.this) {
                    lockedValues.add(value);
                }
                try {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    if (exception != null) {
                        throw new RuntimeException(exception);
                    }
                    return System.currentTimeMillis() - begin;
                } finally {
                    synchronized (LockBean.this) {
                        lockedValues.remove(value);
                    }
                    lock.unlock(value);
                }
            });
        }
    }
}
