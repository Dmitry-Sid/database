package sample.model;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import sample.model.lock.LockService;
import sample.model.lock.ReadWriteLock;
import sample.model.pojo.RowAddress;

import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class RowIdRepositoryImpl implements RowIdRepository {
    private static final Logger log = LoggerFactory.getLogger(RowIdRepositoryImpl.class);
    private final ReadWriteLock<String> readWriteLock = LockService.createReadWriteLock(String.class);

    private final ObjectConverter objectConverter;
    private final Variables variables;
    private final String filesIdPath;
    private final String filesRowPath;
    private final int maxIdSize;
    private final int compressSize;

    public RowIdRepositoryImpl(ObjectConverter objectConverter, String variablesFileName, String filesIdPath, String filesRowPath, int maxIdSize, int compressSize) {
        this.objectConverter = objectConverter;
        this.filesRowPath = filesRowPath;
        this.compressSize = compressSize;
        if (new File(variablesFileName).exists()) {
            this.variables = objectConverter.fromFile(Variables.class, variablesFileName);
        } else {
            this.variables = new Variables(new AtomicInteger(0), Collections.synchronizedSet(new LinkedHashSet<>()),
                    new RowIdRepositoryImpl.CachedRowAddresses(filesIdPath + 1, new ConcurrentHashMap<>(), null));
        }
        this.filesIdPath = filesIdPath;
        this.maxIdSize = maxIdSize;
    }

    @Override
    public int newId() {
        return this.variables.lastId.incrementAndGet();
    }

    @Override
    public void add(int id, Consumer<RowAddress> rowAddressConsumer) {
        final String fileName = getRowIdFileName(id);
        LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Write, fileName, () -> {
            final boolean created = !variables.idBatches.contains(getRowIdFileNumber(id));
            final RowAddress rowAddress;
            if (created) {
                variables.idBatches.add(getRowIdFileNumber(id));
                final Map<Integer, RowAddress> rowAddressMap = emptyRowAddressMap();
                rowAddress = createRowAddress(id);
                rowAddressConsumer.accept(rowAddress);
                rowAddressMap.put(id, rowAddress);
                cacheAndSaveRowAddresses(new CachedRowAddresses(fileName, rowAddressMap, rowAddress));
            } else {
                RowAddress rowAddressFromMap = cacheAndGetRowAddress(id);
                if (rowAddressFromMap == null) {
                    rowAddress = createRowAddress(id);
                    if (variables.cachedRowAddresses.lastRowAddress != null &&
                            variables.cachedRowAddresses.lastRowAddress.getFilePath().equals(getRowFileName(id))) {
                        final RowAddress lastRowAddress = variables.cachedRowAddresses.lastRowAddress;
                        lastRowAddress.setNext(rowAddress);
                        rowAddress.setPosition(lastRowAddress.getPosition() + lastRowAddress.getSize());
                        rowAddress.setPrevious(lastRowAddress);
                    }
                    rowAddressConsumer.accept(rowAddress);
                    variables.cachedRowAddresses.lastRowAddress = rowAddress;
                } else {
                    rowAddress = rowAddressFromMap;
                    final int sizeBefore = rowAddress.getSize();
                    rowAddressConsumer.accept(rowAddress);
                    transform(rowAddress.getId(), sizeBefore, rowAddress.getSize());
                }
                variables.cachedRowAddresses.rowAddressMap.put(rowAddress.getId(), rowAddress);
            }
        });
    }

    private RowAddress createRowAddress(int id) {
        return new RowAddress(getRowFileName(id), id, 0, 0);
    }

    private void transform(int id, int sizeBefore, int sizeAfter) {
        if (sizeBefore == sizeAfter) {
            return;
        }
        RowAddress rowAddressNext = cacheAndGetRowAddress(id);
        if (rowAddressNext == null) {
            throw new RuntimeException("cannot find rowAddress with id : " + id);
        }
        rowAddressNext.setSize(sizeAfter);
        while ((rowAddressNext = rowAddressNext.getNext()) != null) {
            rowAddressNext.setPosition(rowAddressNext.getPosition() + (sizeAfter - sizeBefore));
        }
    }

    @Override
    public void stream(Consumer<RowAddress> rowAddressConsumer, AtomicBoolean stopChecker, Set<Integer> idSet) {
        if (idSet == null) {
            for (Integer value : variables.idBatches) {
                if (stopChecker.get()) {
                    return;
                }
                final String fileName = filesIdPath + value;
                LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Read, fileName, () -> {
                    if (!variables.cachedRowAddresses.fileName.equals(fileName)) {
                        loadCachedRowAddresses(fileName);
                    }
                    stream(this.variables.cachedRowAddresses.rowAddressMap, rowAddressConsumer, stopChecker);
                });
            }
        } else {
            for (Integer id : idSet.stream().sorted(Integer::compareTo)
                    .collect(Collectors.toCollection(LinkedHashSet::new))) {
                final String fileName = getRowIdFileName(id);
                LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Read, fileName, () -> {
                    final RowAddress rowAddress = cacheAndGetRowAddress(id);
                    if (rowAddress != null) {
                        rowAddressConsumer.accept(cacheAndGetRowAddress(id));
                    }
                });
            }
        }
    }

    private void stream(Map<Integer, RowAddress> rowAddressMap, Consumer<RowAddress> rowAddressConsumer, AtomicBoolean stopChecker) {
        for (Map.Entry<Integer, RowAddress> entry : rowAddressMap.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey))
                .collect(Collectors.toCollection(LinkedHashSet::new))) {
            if (stopChecker.get()) {
                return;
            }
            rowAddressConsumer.accept(entry.getValue());
        }
    }

    @Override
    public void delete(int id) {
        final String fileName = getRowIdFileName(id);
        LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Write, fileName, () -> {
            final RowAddress rowAddress = cacheAndGetRowAddress(id);
            if (rowAddress == null) {
                log.warn("rowAddress not found, id : " + id);
                return;
            }
            transform(id, rowAddress.getSize(), 0);
            if (rowAddress.getPrevious() != null) {
                rowAddress.getPrevious().setNext(rowAddress.getNext());
            }
            if (rowAddress.getNext() != null) {
                rowAddress.getNext().setPrevious(rowAddress.getPrevious());
            }
            variables.cachedRowAddresses.rowAddressMap.remove(id);
            if (variables.cachedRowAddresses.rowAddressMap.isEmpty()) {
                variables.idBatches.remove(getRowIdFileNumber(id));
            }
        });
        readWriteLock.writeLock(fileName);
    }

    @Override
    public boolean process(int id, Consumer<RowAddress> consumer) {
        return LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Read, getRowIdFileName(id), () -> {
            final RowAddress rowAddress = cacheAndGetRowAddress(id);
            if (rowAddress == null) {
                return false;
            }
            consumer.accept(rowAddress);
            return true;
        });
    }

    private void cacheAndSaveRowAddresses(CachedRowAddresses cachedRowAddresses) {
        if (cachedRowAddresses == null) {
            return;
        }
        LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Write, cachedRowAddresses.fileName, () -> {
            variables.cachedRowAddresses = cachedRowAddresses;
            objectConverter.toFile(cachedRowAddresses, cachedRowAddresses.fileName);
        });
    }

    private void loadCachedRowAddresses(String fileName) {
        if (!new File(fileName).exists()) {
            return;
        }
        LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Read, fileName, () -> {
            if (this.variables.cachedRowAddresses.fileName.equals(fileName)) {
                return;
            }
            cacheAndSaveRowAddresses(this.variables.cachedRowAddresses);
            this.variables.cachedRowAddresses = objectConverter.fromFile(CachedRowAddresses.class, fileName);
        });
    }

    private RowAddress cacheAndGetRowAddress(int id) {
        final String fileName = getRowIdFileName(id);
        return LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Read, fileName, () -> {
            RowAddress rowAddress = variables.cachedRowAddresses.rowAddressMap.get(id);
            if (rowAddress == null && !fileName.equals(variables.cachedRowAddresses.fileName)) {
                loadCachedRowAddresses(fileName);
                rowAddress = variables.cachedRowAddresses.rowAddressMap.get(id);
            }
            return rowAddress;
        });
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
        private RowAddress lastRowAddress;

        public CachedRowAddresses(String fileName, Map<Integer, RowAddress> rowAddressMap, RowAddress lastRowAddress) {
            this.fileName = fileName;
            this.rowAddressMap = rowAddressMap;
            this.lastRowAddress = lastRowAddress;
        }
    }
}
