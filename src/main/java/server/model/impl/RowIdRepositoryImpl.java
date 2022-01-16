package server.model.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.model.*;
import server.model.lock.Lock;
import server.model.lock.LockService;
import server.model.lock.ReadWriteLock;
import server.model.pojo.RowAddress;

import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RowIdRepositoryImpl extends BaseDestroyable implements RowIdRepository {
    private static final Logger log = LoggerFactory.getLogger(RowIdRepositoryImpl.class);
    private static final String ROW_NAME = "row";
    private static final String ROW_ID_NAME = "rowId";
    private static final String VARIABLES_NAME = "rowIdVariables";

    private final ReadWriteLock<String> readWriteLock = LockService.createReadWriteLock(String.class);
    private final Lock<String> rowIdLock = LockService.createLock(String.class);
    private final Variables variables;
    private final String variablesFileName;
    private final String filesIdPath;
    private final String filesRowPath;
    private final int maxIdSize;
    private final int compressSize;
    private volatile boolean changed;

    public RowIdRepositoryImpl(String filePath, boolean init, ObjectConverter objectConverter, DestroyService destroyService, int maxIdSize, int compressSize) {
        super(filePath, init, objectConverter, destroyService, Utils.getFullPath(filePath, ROW_ID_NAME), Utils.getFullPath(filePath, ROW_NAME));
        this.filesIdPath = Utils.getFullPath(filePath, ROW_ID_NAME) + ROW_ID_NAME;
        this.filesRowPath = Utils.getFullPath(filePath, ROW_NAME) + ROW_NAME;
        this.variablesFileName = Utils.getFullPath(filePath, ROW_ID_NAME) + VARIABLES_NAME;
        this.compressSize = compressSize;
        final File file = new File(this.variablesFileName);
        if (file.exists()) {
            this.variables = objectConverter.fromFile(Variables.class, this.variablesFileName);
        } else {
            this.variables = new Variables(new AtomicInteger(0), new CopyOnWriteArraySet<>(), new ConcurrentHashMap<>());
        }
        this.maxIdSize = maxIdSize;
    }

    @Override
    public int newId() {
        changed = true;
        return variables.lastId.incrementAndGet();
    }

    @Override
    public void add(int id, Consumer<RowAddress> rowAddressConsumer) {
        add(id, false, rowAddressConsumer);
    }

    @Override
    public void save(int id, Consumer<RowAddress> rowAddressConsumer) {
        add(id, true, rowAddressConsumer);
    }

    public void add(int id, boolean save, Consumer<RowAddress> rowAddressConsumer) {
        final String fileName = getRowIdFileName(id);
        processRowAddresses(readWriteLock.writeLock(), fileName, true, cachedRowAddresses -> {
            if (cachedRowAddresses == null) {
                final int rowIdFileNumber = getRowIdFileNumber(id);
                if (variables.idBatches.contains(rowIdFileNumber)) {
                    throw new IllegalStateException("idBatches contains rowIdFileNumber " + rowIdFileNumber);
                }
                variables.idBatches.add(rowIdFileNumber);
                final Map<Integer, RowAddress> rowAddressMap = emptyRowAddressMap();
                final RowAddress rowAddress = createRowAddress(id);
                rowAddressConsumer.accept(rowAddress);
                rowAddress.setSaved(save);
                rowAddressMap.put(id, rowAddress);
                variables.cachedRowAddressesMap.put(fileName, new CachedRowAddresses(rowAddressMap, rowAddress));
            } else {
                final RowAddress rowAddressFromMap = cachedRowAddresses.rowAddressMap.get(id);
                final RowAddress rowAddress;
                if (rowAddressFromMap == null) {
                    rowAddress = createRowAddress(id);
                    if (cachedRowAddresses.lastRowAddress != null && cachedRowAddresses.lastRowAddress.getFilePath().equals(getRowFileName(id))) {
                        final RowAddress lastRowAddress = cachedRowAddresses.lastRowAddress;
                        lastRowAddress.setNext(rowAddress.getId());
                        rowAddress.setPosition(lastRowAddress.getPosition() + lastRowAddress.getSize());
                        rowAddress.setPrevious(lastRowAddress.getId());
                    }
                    rowAddressConsumer.accept(rowAddress);
                    cachedRowAddresses.lastRowAddress = rowAddress;
                } else {
                    if (!save) {
                        throw new IllegalArgumentException("cannot add existing rowId " + id);
                    }
                    rowAddress = rowAddressFromMap;
                    final int sizeBefore = rowAddress.getSize();
                    rowAddressConsumer.accept(rowAddress);
                    transform(cachedRowAddresses, rowAddress, sizeBefore, rowAddress.getSize());
                }
                rowAddress.setSaved(save);
                cachedRowAddresses.rowAddressMap.put(rowAddress.getId(), rowAddress);
            }
        });
        changed = true;
    }

    @Override
    public StoppableBatchStream<RowAddress> batchStream() {
        return new BatchStream(true);
    }

    @Override
    public StoppableBatchStream<RowAddress> batchStream(Set<Integer> idSet, StreamType streamType) {
        return new BatchStream(idSet, streamType);
    }

    private RowAddress createRowAddress(int id) {
        return new RowAddress(getRowFileName(id), id, 0, 0);
    }

    private void transform(CachedRowAddresses cachedRowAddresses, RowAddress rowAddress, int sizeBefore, int sizeAfter) {
        if (sizeBefore == sizeAfter) {
            return;
        }
        RowAddress rowAddressNext = rowAddress;
        rowAddressNext.setSize(sizeAfter);
        while ((rowAddressNext = cachedRowAddresses.rowAddressMap.get(rowAddressNext.getNext())) != null) {
            rowAddressNext.setPosition(rowAddressNext.getPosition() + (sizeAfter - sizeBefore));
        }
    }

    @Override
    public void delete(int id) {
        final String rowIdFileName = getRowIdFileName(id);
        processRowAddresses(readWriteLock.writeLock(), rowIdFileName, false, cachedRowAddresses -> {
            final RowAddress rowAddress = cachedRowAddresses.rowAddressMap.get(id);
            if (rowAddress == null) {
                throw new IllegalStateException("rowAddress not found, id : " + id);
            }
            transform(cachedRowAddresses, rowAddress, rowAddress.getSize(), 0);
            if (cachedRowAddresses.rowAddressMap.get(rowAddress.getPrevious()) != null) {
                cachedRowAddresses.rowAddressMap.get(rowAddress.getPrevious()).setNext(rowAddress.getNext());
            }
            if (cachedRowAddresses.rowAddressMap.get(rowAddress.getNext()) != null) {
                cachedRowAddresses.rowAddressMap.get(rowAddress.getNext()).setPrevious(rowAddress.getPrevious());
            }
            cachedRowAddresses.rowAddressMap.remove(id);
            if (cachedRowAddresses.rowAddressMap.isEmpty()) {
                variables.idBatches.remove(getRowIdFileNumber(id));
                variables.cachedRowAddressesMap.remove(rowIdFileName);
                if (!new File(rowIdFileName).delete()) {
                    throw new RuntimeException("cannot delete file " + rowIdFileName);
                }
            }
            changed = true;
        });
    }

    @Override
    public boolean process(int id, Consumer<RowAddress> consumer) {
        return process(id, false, consumer);
    }

    private boolean process(int id, boolean includeAll, Consumer<RowAddress> consumer) {
        final AtomicBoolean processed = new AtomicBoolean();
        processRowAddresses(readWriteLock.readLock(), getRowIdFileName(id), false, cachedRowAddresses -> {
            final RowAddress rowAddress = cachedRowAddresses.rowAddressMap.get(id);
            if (rowAddress != null && (rowAddress.isSaved() || includeAll)) {
                consumer.accept(objectConverter.clone(rowAddress));
                processed.set(true);
            }
        });
        return processed.get();
    }

    @Override
    public String getRowFileName(int rowId) {
        return filesRowPath + getRowFileNumber(rowId);
    }

    private int getRowIdFileNumber(int id) {
        return 1 + (id - 1) / maxIdSize;
    }

    private int getRowFileNumber(int id) {
        return 1 + (id - 1) * compressSize / maxIdSize;
    }

    private String getRowIdFileName(int id) {
        return filesIdPath + getRowIdFileNumber(id);
    }

    private Map<Integer, RowAddress> emptyRowAddressMap() {
        return new ConcurrentHashMap<>();
    }

    private void processRowAddresses(Lock<String> lock, String fileName, boolean acceptNull, Consumer<CachedRowAddresses> consumer) {
        try {
            LockService.doInLock(lock, fileName, () -> {
                CachedRowAddresses cachedRowAddresses = variables.cachedRowAddressesMap.get(fileName);
                if (cachedRowAddresses == null) {
                    rowIdLock.lock(fileName);
                    try {
                        cachedRowAddresses = variables.cachedRowAddressesMap.get(fileName);
                        if (cachedRowAddresses == null) {
                            cachedRowAddresses = objectConverter.fromFile(CachedRowAddresses.class, fileName);
                            if (cachedRowAddresses != null) {
                                variables.cachedRowAddressesMap.put(fileName, cachedRowAddresses);
                            }
                        }
                    } finally {
                        rowIdLock.unlock(fileName);
                    }
                }
                if (acceptNull || cachedRowAddresses != null) {
                    consumer.accept(cachedRowAddresses);
                }
            });
        } catch (Exception e) {
            log.error("failed process rowAddresses file " + fileName, e);
            throw e;
        }
    }

    private void saveAndClearMap() {
        if (variables.cachedRowAddressesMap == null) {
            return;
        }
        final Set<Map.Entry<String, CachedRowAddresses>> entrySet = variables.cachedRowAddressesMap.entrySet();
        for (Iterator<Map.Entry<String, CachedRowAddresses>> iterator = entrySet.iterator(); iterator.hasNext(); ) {
            final Map.Entry<String, CachedRowAddresses> entry = iterator.next();
            LockService.doInLock(readWriteLock.writeLock(), entry.getKey(), () -> {
                final CachedRowAddresses cachedRowAddresses = entry.getValue();
                if (!cachedRowAddresses.rowAddressMap.isEmpty() && changed) {
                    objectConverter.toFile(entry.getValue(), entry.getKey());
                }
                if (entrySet.size() > 1 || cachedRowAddresses.rowAddressMap.isEmpty()) {
                    iterator.remove();
                }
            });
        }
    }

    @Override
    public void destroy() {
        saveAndClearMap();
        if (changed) {
            objectConverter.toFile(variables, variablesFileName);
            changed = false;
        }
    }

    private Set<Integer> sortedSet(Set<Integer> set) {
        return set.stream().sorted(Integer::compareTo)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    public static class Variables implements Serializable {
        private static final long serialVersionUID = 1228422981455428546L;
        private final AtomicInteger lastId;
        private final Set<Integer> idBatches;
        private Map<String, CachedRowAddresses> cachedRowAddressesMap;

        public Variables(AtomicInteger lastId, Set<Integer> idBatches, Map<String, CachedRowAddresses> cachedRowAddressesMap) {
            this.lastId = lastId;
            this.idBatches = idBatches;
            this.cachedRowAddressesMap = cachedRowAddressesMap;
        }
    }

    public static class CachedRowAddresses implements Serializable {
        private static final long serialVersionUID = 6741511503259730900L;
        private final Map<Integer, RowAddress> rowAddressMap;
        private volatile RowAddress lastRowAddress;

        public CachedRowAddresses(Map<Integer, RowAddress> rowAddressMap, RowAddress lastRowAddress) {
            this.rowAddressMap = rowAddressMap;
            this.lastRowAddress = lastRowAddress;
        }
    }

    private class BatchStream extends BaseStoppableBatchStream<RowAddress> {
        private final boolean full;
        private final Set<Integer> idSet;
        private final Lock<String> lock;

        private BatchStream(boolean full) {
            this.full = full;
            this.idSet = null;
            this.lock = readWriteLock.readLock();
        }

        private BatchStream(Set<Integer> idSet, StreamType streamType) {
            this.full = false;
            this.idSet = idSet;
            this.lock = streamType == StreamType.Write ? readWriteLock.writeLock() : readWriteLock.readLock();
        }

        @Override
        public void forEach(Consumer<RowAddress> consumer) {
            if (full) {
                forEach(variables.idBatches, value -> filesIdPath + value, value -> processRowAddresses(lock, filesIdPath + value, false,
                        cachedRowAddresses -> stream(cachedRowAddresses.rowAddressMap, consumer, stopChecker)));
            } else {
                forEach(idSet, RowIdRepositoryImpl.this::getRowIdFileName, value -> process(value, true, consumer));
            }
        }

        private void forEach(Set<Integer> set, Function<Integer, String> fileFunction, Consumer<Integer> consumer) {
            if (set == null) {
                return;
            }
            final String[] fileName = {null};
            try (ChainedLock<String> chainedLock = new ChainedLock<>(lock)) {
                for (int value : sortedSet(set)) {
                    if (stopChecker.get()) {
                        return;
                    }
                    Utils.compareAndRun(fileFunction.apply(value), fileName[0], () -> {
                        if (fileName[0] != null) {
                            onBatchEnd.forEach(Runnable::run);
                        }
                        fileName[0] = fileFunction.apply(value);
                        chainedLock.init(fileName[0]);
                    });
                    consumer.accept(value);
                }
                if (fileName[0] != null) {
                    onStreamEnd.forEach(Runnable::run);
                    onBatchEnd.forEach(Runnable::run);
                }
            }
        }

        private void stream(Map<Integer, RowAddress> rowAddressMap, Consumer<RowAddress> rowAddressConsumer, AtomicBoolean stopChecker) {
            for (Map.Entry<Integer, RowAddress> entry : rowAddressMap.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey))
                    .collect(Collectors.toCollection(LinkedHashSet::new))) {
                if (stopChecker != null && stopChecker.get()) {
                    return;
                }
                if (entry.getValue().isSaved()) {
                    rowAddressConsumer.accept(objectConverter.clone(entry.getValue()));
                }
            }
        }
    }
}