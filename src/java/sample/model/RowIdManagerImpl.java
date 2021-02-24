package sample.model;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import sample.model.pojo.RowAddress;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

public class RowIdManagerImpl implements RowIdManager {
    private static final Logger log = LoggerFactory.getLogger(RowIdManagerImpl.class);

    private final ObjectConverter objectConverter;
    private final Object CACHED_LOCK = new Object();
    private final Variables variables;
    private final int maxSize;
    private final String filesIdPath;
    private final String filesRowPath;

    public RowIdManagerImpl(ObjectConverter objectConverter, int maxSize, String variablesFileName, String filesIdPath, String filesRowPath) {
        this.objectConverter = objectConverter;
        this.variables = objectConverter.fromFile(Variables.class, variablesFileName);
        this.maxSize = maxSize;
        this.filesIdPath = filesIdPath;
        this.filesRowPath = filesRowPath;
    }

    @Override
    public boolean process(int id, Consumer<RowAddress> rowAddressConsumer) {
        if (id <= 0) {
            return false;
        }
        final String fileName = getRowIdFileName(id);
        if (fileName == null) {
            return false;
        }
        return LockService.doInFileLock(fileName, () -> {
            final RowAddress rowAddress = cacheAndGetRowAddress(id, fileName);
            if (rowAddress == null) {
                return false;
            }
            LockService.doInRowIdLock(rowAddress.getId(), () -> {
                rowAddressConsumer.accept(rowAddress);
                return null;
            });
            return true;
        });
    }

    @Override
    public void transform(int id, int newSize) {
        if (newSize < 0) {
            throw new RuntimeException("new size must be not negative");
        }
        synchronized (CACHED_LOCK) {
            if (variables.cachedRowAddresses.rowAddressMap.containsKey(id)) {
                LockService.doInFileLock(variables.cachedRowAddresses.fileName, () -> {
                    transform(id, newSize, variables.cachedRowAddresses.rowAddressMap);
                    objectConverter.toFile((Serializable) variables.cachedRowAddresses.rowAddressMap, variables.cachedRowAddresses.fileName);
                    return null;
                });
                return;
            }
        }
        final String fileName = getRowIdFileName(id);
        if (fileName == null) {
            throw new RuntimeException("cannot find rowAddress with id : " + id);
        }
        LockService.doInFileLock(variables.cachedRowAddresses.fileName, () -> {
            final Map<Integer, RowAddress> map = getRowAddressesFromFile(fileName);
            transform(id, newSize, map);
            objectConverter.toFile((Serializable) map, fileName);
            return null;
        });
    }

    private void transform(int id, int sizeAfter, Map<Integer, RowAddress> rowAddressMap) {
        RowAddress rowAddressNext = rowAddressMap.get(id);
        if (rowAddressNext == null) {
            throw new RuntimeException("cannot find rowAddress with id : " + id);
        }
        final int sizeBefore = rowAddressNext.getSize();
        rowAddressNext.setSize(sizeAfter);
        while ((rowAddressNext = rowAddressNext.getNext()) != null) {
            rowAddressNext.setPosition(rowAddressNext.getPosition() + (sizeAfter - sizeBefore));
        }
    }

    @Override
    public void add(Function<RowAddress, Boolean> function) {
        synchronized (variables.lastId) {
            final int lastId = variables.lastId.get();
            LockService.doInRowIdLock(lastId, () -> {
                final int id = variables.lastId.incrementAndGet();
                try {
                    LockService.doInRowIdLock(id, () -> {
                        String fileName;
                        final AtomicBoolean created = new AtomicBoolean(false);
                        synchronized (variables.idBatchMap) {
                            fileName = variables.idBatchMap.get(getBounds(id));
                            if (fileName == null) {
                                fileName = getNewRowIdFileName(id);
                                created.set(true);
                            }
                        }
                        final String finalFileName = fileName;
                        LockService.doInFileLock(fileName, () -> {
                            if (!created.get()) {
                                synchronized (CACHED_LOCK) {
                                    final RowAddress lastRowAddress = cacheAndGetRowAddress(lastId, finalFileName);
                                    if (variables.cachedRowAddresses.rowAddressMap.containsKey(id)) {
                                        throw new RuntimeException("already has same id : " + id);
                                    }
                                    final RowAddress rowAddress = new RowAddress(lastRowAddress.getFilePath(), id,
                                            lastRowAddress.getPosition() + lastRowAddress.getSize(), 0);
                                    lastRowAddress.setNext(rowAddress);
                                    rowAddress.setPrevious(lastRowAddress);
                                    if (function.apply(rowAddress)) {
                                        variables.cachedRowAddresses.rowAddressMap.put(id, rowAddress);
                                        objectConverter.toFile((Serializable) variables.cachedRowAddresses.rowAddressMap, finalFileName);
                                    }
                                }
                            } else {
                                final RowAddress rowAddress = new RowAddress(getNewRowFileName(id), id, 1, 0);
                                if (function.apply(rowAddress)) {
                                    synchronized (variables.idBatchMap) {
                                        variables.idBatchMap.put(getBounds(id), finalFileName);
                                    }
                                    final Map<Integer, RowAddress> rowAddressMap = emptyRowAddressMap();
                                    rowAddressMap.put(id, rowAddress);
                                    objectConverter.toFile((Serializable) rowAddressMap, finalFileName);
                                }
                            }
                            return null;
                        });
                        return null;
                    });
                } catch (Throwable throwable) {
                    variables.lastId.decrementAndGet();
                    throw throwable;
                }
                return null;
            });
        }
    }

    @Override
    public void delete(int id) {
        LockService.doInRowIdLock(id, () -> {
            final String fileName = getRowIdFileName(id);
            LockService.doInFileLock(fileName, () -> {
                synchronized (CACHED_LOCK) {
                    final RowAddress rowAddress = cacheAndGetRowAddress(id, fileName);
                    if (rowAddress == null) {
                        log.warn("rowAddress not found, id : " + id);
                        return null;
                    }
                    transform(id, 0, variables.cachedRowAddresses.rowAddressMap);
                    if (rowAddress.getPrevious() != null) {
                        rowAddress.getPrevious().setNext(rowAddress.getNext());
                    }
                    if (rowAddress.getNext() != null) {
                        rowAddress.getNext().setPrevious(rowAddress.getPrevious());
                    }
                    synchronized (variables.lastId) {
                        variables.cachedRowAddresses.rowAddressMap.remove(id);
                        objectConverter.toFile((Serializable) variables.cachedRowAddresses.rowAddressMap, fileName);
                        if (id == variables.lastId.get()) {
                            variables.lastId.decrementAndGet();
                        }
                    }
                }
                return null;
            });
            return null;
        });
    }

    private RowAddress cacheAndGetRowAddress(int id, String fileName) {
        synchronized (CACHED_LOCK) {
            RowAddress rowAddress = variables.cachedRowAddresses.rowAddressMap.get(id);
            if (rowAddress == null && !fileName.equals(variables.cachedRowAddresses.fileName)) {
                variables.cachedRowAddresses = new CachedRowAddresses(fileName, getRowAddressesFromFile(fileName));
                rowAddress = variables.cachedRowAddresses.rowAddressMap.get(id);
            }
            return rowAddress;
        }
    }

    private int getFileNumber(int id) {
        return id / maxSize;
    }


    private String getNewRowFileName(int id) {
        return filesIdPath + getFileNumber(id);
    }

    private String getNewRowIdFileName(int id) {
        return filesIdPath + getFileNumber(id);
    }

    private String getRowIdFileName(int id) {
        synchronized (variables.idBatchMap) {
            return variables.idBatchMap.get(getBounds(id));
        }
    }

    private IdBounds getBounds(int id) {
        final int lowBound = maxSize * getFileNumber(id) + 1;
        return new IdBounds(lowBound, lowBound + maxSize - 1);
    }

    public Map<Integer, RowAddress> getRowAddressesFromFile(String fileName) {
        synchronized (CACHED_LOCK) {
            return objectConverter.fromFile(variables.cachedRowAddresses.rowAddressMap.getClass(), fileName);
        }
    }

    private Map<Integer, RowAddress> emptyRowAddressMap() {
        return new ConcurrentHashMap<>();
    }

    public static class IdBounds implements Serializable {
        private static final long serialVersionUID = -6498812036951981474L;
        public final int lowBound;
        public final int highBound;

        public IdBounds(int lowBound, int highBound) {
            this.lowBound = lowBound;
            this.highBound = highBound;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof IdBounds)) {
                return false;
            }
            final IdBounds idBounds = (IdBounds) o;
            return lowBound == idBounds.lowBound &&
                    highBound == idBounds.highBound;
        }

        @Override
        public int hashCode() {
            return Objects.hash(lowBound, highBound);
        }
    }

    public static class Variables implements Serializable {
        private static final long serialVersionUID = 1228422981455428546L;
        private final AtomicInteger lastId;
        private final Map<IdBounds, String> idBatchMap;
        private CachedRowAddresses cachedRowAddresses;

        public Variables(AtomicInteger lastId, Map<IdBounds, String> idBatchMap, CachedRowAddresses cachedRowAddresses) {
            this.lastId = lastId;
            this.idBatchMap = idBatchMap;
            this.cachedRowAddresses = cachedRowAddresses;
        }
    }

    public static class CachedRowAddresses implements Serializable {
        private static final long serialVersionUID = 6741511503259730900L;
        private final String fileName;
        private final Map<Integer, RowAddress> rowAddressMap;

        public CachedRowAddresses(String fileName, Map<Integer, RowAddress> rowAddressMap) {
            this.fileName = fileName;
            this.rowAddressMap = rowAddressMap;
        }
    }
}
