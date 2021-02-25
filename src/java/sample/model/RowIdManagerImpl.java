package sample.model;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import sample.model.pojo.RowAddress;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

public class RowIdManagerImpl implements RowIdManager {
    private static final Logger log = LoggerFactory.getLogger(RowIdManagerImpl.class);

    private final ObjectConverter objectConverter;
    private final Object CACHED_LOCK = new Object();
    private final Variables variables;
    private final int maxIdSize;
    private final int compressSize;
    private final String filesIdPath;
    private final String filesRowPath;

    public RowIdManagerImpl(ObjectConverter objectConverter, int maxIdSize, int compressSize, String variablesFileName, String filesIdPath, String filesRowPath) {
        this.objectConverter = objectConverter;
        this.compressSize = compressSize;
        this.variables = objectConverter.fromFile(Variables.class, variablesFileName);
        this.maxIdSize = maxIdSize;
        this.filesIdPath = filesIdPath;
        this.filesRowPath = filesRowPath;
    }

    @Override
    public boolean process(int id, Consumer<RowAddress> rowAddressConsumer) {
        if (id <= 0) {
            return false;
        }
        if (!variables.idBatches.contains(getRowIdFileNumber(id))) {
            return false;
        }
        final String fileName = getRowIdFileName(id);
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
        LockService.doInRowIdLock(id, () -> {
            synchronized (CACHED_LOCK) {
                if (variables.cachedRowAddresses.rowAddressMap.containsKey(id)) {
                    LockService.doInFileLock(variables.cachedRowAddresses.fileName, () -> {
                        transform(id, newSize, variables.cachedRowAddresses.rowAddressMap);
                        objectConverter.toFile((Serializable) variables.cachedRowAddresses.rowAddressMap, variables.cachedRowAddresses.fileName);
                        return null;
                    });
                    return null;
                }
            }
            if (!variables.idBatches.contains(getRowIdFileNumber(id))) {
                throw new RuntimeException("cannot find rowAddress with id : " + id);
            }
            final String fileName = getRowIdFileName(id);
            LockService.doInFileLock(fileName, () -> {
                final Map<Integer, RowAddress> map = getRowAddressesFromFile(fileName);
                transform(id, newSize, map);
                objectConverter.toFile((Serializable) map, fileName);
                return null;
            });
            return null;
        });
    }

    private void transform(int id, int sizeAfter, Map<Integer, RowAddress> rowAddressMap) {
        LockService.doInRowIdLock(id, () -> {
            RowAddress rowAddressNext = rowAddressMap.get(id);
            if (rowAddressNext == null) {
                throw new RuntimeException("cannot find rowAddress with id : " + id);
            }
            final int sizeBefore = rowAddressNext.getSize();
            rowAddressNext.setSize(sizeAfter);
            while ((rowAddressNext = rowAddressNext.getNext()) != null) {
                final RowAddress finalRowAddressNext = rowAddressNext;
                LockService.doInRowIdLock(rowAddressNext.getId(), () -> {
                    finalRowAddressNext.setPosition(finalRowAddressNext.getPosition() + (sizeAfter - sizeBefore));
                    return null;
                });
            }
            return null;
        });
    }

    @Override
    public void add(Function<RowAddress, Boolean> function) {
        synchronized (variables.lastId) {
            final int lastId = variables.lastId.get();
            LockService.doInRowIdLock(lastId, () -> {
                final int id = variables.lastId.incrementAndGet();
                try {
                    LockService.doInRowIdLock(id, () -> {
                        final String fileName = getRowIdFileName(id);
                        final boolean created = !variables.idBatches.contains(getRowIdFileNumber(id));
                        LockService.doInFileLock(fileName, () -> {
                            final RowAddress rowAddress = new RowAddress(getRowFileName(id), id, 0, 0);
                            final Runnable actionFalse = variables.lastId::decrementAndGet;
                            if (created) {
                                doOrAnotherAction(function.apply(rowAddress), () -> {
                                    variables.idBatches.add(getRowIdFileNumber(id));
                                    final Map<Integer, RowAddress> rowAddressMap = emptyRowAddressMap();
                                    rowAddressMap.put(id, rowAddress);
                                    objectConverter.toFile((Serializable) rowAddressMap, fileName);
                                }, actionFalse);
                            } else {
                                synchronized (CACHED_LOCK) {
                                    final RowAddress lastRowAddress = cacheAndGetRowAddress(lastId, fileName);
                                    if (variables.cachedRowAddresses.rowAddressMap.containsKey(id)) {
                                        throw new RuntimeException("already has same id : " + id);
                                    }
                                    if (lastRowAddress.getFilePath().equals(rowAddress.getFilePath())) {
                                        rowAddress.setPosition(lastRowAddress.getPosition() + lastRowAddress.getSize());
                                        lastRowAddress.setNext(rowAddress);
                                        rowAddress.setPrevious(lastRowAddress);
                                    }
                                    doOrAnotherAction(function.apply(rowAddress), () -> {
                                        variables.cachedRowAddresses.rowAddressMap.put(id, rowAddress);
                                        objectConverter.toFile((Serializable) variables.cachedRowAddresses.rowAddressMap, fileName);
                                    }, actionFalse);
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

    private void doOrAnotherAction(boolean flag, Runnable actionTrue, Runnable actionFalse) {
        if (flag) {
            actionTrue.run();
        } else {
            actionFalse.run();
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
                    variables.cachedRowAddresses.rowAddressMap.remove(id);
                    if (variables.cachedRowAddresses.rowAddressMap.isEmpty()) {
                        variables.idBatches.remove(getRowIdFileNumber(id));
                    } else {
                        objectConverter.toFile((Serializable) variables.cachedRowAddresses.rowAddressMap, fileName);
                    }
                }
                variables.lastId.compareAndSet(id, id - 1);
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

    private int getRowIdFileNumber(int id) {
        return 1 + (id - 1) / maxIdSize;
    }

    private int getRowFileNumber(int id) {
        return 1 + (id - 1) * compressSize / maxIdSize;
    }

    private String getRowFileName(int id) {
        return filesRowPath + getRowFileNumber(id);
    }

    private String getRowIdFileName(int id) {
        return filesIdPath + getRowIdFileNumber(id);
    }

    private Map<Integer, RowAddress> getRowAddressesFromFile(String fileName) {
        synchronized (CACHED_LOCK) {
            return objectConverter.fromFile(variables.cachedRowAddresses.rowAddressMap.getClass(), fileName);
        }
    }

    private Map<Integer, RowAddress> emptyRowAddressMap() {
        return new ConcurrentHashMap<>();
    }

    public static class Variables implements Serializable {
        private static final long serialVersionUID = 1228422981455428546L;
        private final AtomicInteger lastId;
        private final Set<Integer> idBatches;
        private CachedRowAddresses cachedRowAddresses;

        public Variables(AtomicInteger lastId, Set<Integer> idBatches, CachedRowAddresses cachedRowAddresses) {
            this.lastId = lastId;
            this.idBatches = idBatches;
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
