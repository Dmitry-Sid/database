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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class RowIdRepositoryImpl extends BaseDestroyable implements RowIdRepository {
    private static final Logger log = LoggerFactory.getLogger(RowIdRepositoryImpl.class);
    private static final String ROW_NAME = "row";
    private static final String ROW_ID_NAME = "rowId";
    private static final String VARIABLES_NAME = "rowIdVariables";

    private final ReadWriteLock<Integer> rowIdReadWriteLock = LockService.createReadWriteLock(Integer.class);
    private final ReadWriteLock<Integer> rowReadWriteLock = LockService.createReadWriteLock(Integer.class);
    private final Lock<Integer> rowIdLock = LockService.createLock(Integer.class);
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
            this.variables = new Variables(new AtomicInteger(0), new CopyOnWriteArraySet<>());
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
        processRowAddresses(rowReadWriteLock.writeLock(), id, true, (rowFileNumber, rowAddresses) -> {
            if (rowAddresses == null) {
                final int rowIdFileNumber = getRowIdFileNumber(id);
                if (variables.idBatches.contains(rowIdFileNumber)) {
                    throw new IllegalStateException("idBatches contains rowIdFileNumber " + rowIdFileNumber);
                }
                variables.idBatches.add(rowIdFileNumber);
                final RowAddress rowAddress = createRowAddress(id);
                rowAddressConsumer.accept(rowAddress);
                rowAddress.setSaved(save);
                final RowAddresses createdRowAddresses = new RowAddresses();
                final RowAddressBasket rowAddressBasket = createdRowAddresses.baskets.computeIfAbsent(rowFileNumber, k -> new RowAddressBasket(rowAddress));
                rowAddressBasket.rowAddressMap.put(id, rowAddress);
                variables.cachedRowAddressesMap.put(rowIdFileNumber, createdRowAddresses);
            } else {
                final RowAddressBasket rowAddressBasket = rowAddresses.baskets.computeIfAbsent(getRowFileNumber(id), k -> new RowAddressBasket());
                final RowAddress rowAddressFromMap = rowAddressBasket.rowAddressMap.get(id);
                final RowAddress rowAddress;
                if (rowAddressFromMap == null) {
                    rowAddress = createRowAddress(id);
                    if (rowAddressBasket.lastRowAddress != null && rowAddressBasket.lastRowAddress.getFilePath().equals(getRowFileName(id))) {
                        final RowAddress lastRowAddress = rowAddressBasket.lastRowAddress;
                        lastRowAddress.setNext(rowAddress.getId());
                        rowAddress.setPosition(lastRowAddress.getPosition() + lastRowAddress.getSize());
                        rowAddress.setPrevious(lastRowAddress.getId());
                    }
                    rowAddressConsumer.accept(rowAddress);
                    rowAddressBasket.lastRowAddress = rowAddress;
                } else {
                    if (!save) {
                        throw new IllegalArgumentException("cannot add existing rowId " + id);
                    }
                    rowAddress = rowAddressFromMap;
                    final int sizeBefore = rowAddress.getSize();
                    rowAddressConsumer.accept(rowAddress);
                    transform(rowAddressBasket, rowAddress, sizeBefore, rowAddress.getSize());
                }
                rowAddress.setSaved(save);
                rowAddressBasket.rowAddressMap.put(rowAddress.getId(), rowAddress);
            }
            changed = true;
        });
    }

    @Override
    public StoppableBatchStream<RowAddress> batchStream() {
        return new BatchStream(true);
    }

    @Override
    public StoppableBatchStream<RowAddress> batchStream(Set<Integer> idSet, ProcessType processType) {
        return new BatchStream(idSet, processType);
    }

    private RowAddress createRowAddress(int id) {
        return new RowAddress(getRowFileName(id), id, 0, 0);
    }

    private void transform(RowAddressBasket rowAddressBasket, RowAddress rowAddress, int sizeBefore, int sizeAfter) {
        if (sizeBefore == sizeAfter) {
            return;
        }
        RowAddress rowAddressNext = rowAddress;
        rowAddressNext.setSize(sizeAfter);
        while ((rowAddressNext = rowAddressBasket.rowAddressMap.get(rowAddressNext.getNext())) != null) {
            rowAddressNext.setPosition(rowAddressNext.getPosition() + (sizeAfter - sizeBefore));
        }
    }

    @Override
    public void delete(int id) {
        processRowAddresses(rowReadWriteLock.writeLock(), id, false, (rowFileNumber, rowAddresses) -> {
            final RowAddressBasket rowAddressBasket = rowAddresses.baskets.get(rowFileNumber);
            if (rowAddressBasket == null) {
                throw new IllegalStateException("rowAddressBasket not found, id : " + id);
            }
            final RowAddress rowAddress = rowAddressBasket.rowAddressMap.get(id);
            if (rowAddress == null) {
                throw new IllegalStateException("rowAddress not found, id : " + id);
            }
            transform(rowAddressBasket, rowAddress, rowAddress.getSize(), 0);
            if (rowAddressBasket.rowAddressMap.get(rowAddress.getPrevious()) != null) {
                rowAddressBasket.rowAddressMap.get(rowAddress.getPrevious()).setNext(rowAddress.getNext());
            }
            if (rowAddressBasket.rowAddressMap.get(rowAddress.getNext()) != null) {
                rowAddressBasket.rowAddressMap.get(rowAddress.getNext()).setPrevious(rowAddress.getPrevious());
            }
            rowAddressBasket.rowAddressMap.remove(id);
            if (rowAddressBasket.rowAddressMap.isEmpty()) {
                rowAddresses.baskets.remove(rowFileNumber);
            }
            changed = true;
        });
    }

    @Override
    public boolean process(int id, Consumer<RowAddress> consumer) {
        final AtomicBoolean processed = new AtomicBoolean();
        processRowAddresses(rowReadWriteLock.readLock(), id, false, (rowFileNumber, rowAddresses) -> {
            final RowAddressBasket rowAddressBasket = rowAddresses.baskets.get(rowFileNumber);
            if (rowAddressBasket == null) {
                return;
            }
            processed.set(process(rowAddressBasket, id, false, consumer));
        });
        return processed.get();
    }

    private boolean process(RowAddressBasket basket, int id, boolean includeAll, Consumer<RowAddress> consumer) {
        final RowAddress rowAddress = basket.rowAddressMap.get(id);
        if (rowAddress != null && (rowAddress.isSaved() || includeAll)) {
            consumer.accept(objectConverter.clone(rowAddress));
            return true;
        }
        return false;
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

    private void processRowAddresses(Lock<Integer> lock, int id, boolean acceptNull, BiConsumer<Integer, RowAddresses> consumer) {
        final int rowIdFileNumber = getRowIdFileNumber(id);
        final int rowFileNumber = getRowFileNumber(id);
        processRowAddresses(rowIdFileNumber, acceptNull, rowAddresses -> LockService.doInLock(lock, rowFileNumber, () -> consumer.accept(rowFileNumber, rowAddresses)));
    }

    private void processRowAddresses(int rowIdFileNumber, boolean acceptNull, Consumer<RowAddresses> consumer) {
        try {
            LockService.doInLock(rowIdReadWriteLock.readLock(), rowIdFileNumber, () -> {
                final RowAddresses rowAddresses = getRowAddresses(rowIdFileNumber);
                if (acceptNull || rowAddresses != null) {
                    consumer.accept(rowAddresses);
                }
            });
        } catch (Exception e) {
            log.error("failed process rowAddresses file " + filesIdPath + rowIdFileNumber, e);
            throw e;
        }
    }

    private RowAddresses getRowAddresses(int rowIdFileNumber) {
        RowAddresses rowAddresses = variables.cachedRowAddressesMap.get(rowIdFileNumber);
        if (rowAddresses == null) {
            rowIdLock.lock(rowIdFileNumber);
            try {
                rowAddresses = variables.cachedRowAddressesMap.get(rowIdFileNumber);
                if (rowAddresses == null) {
                    rowAddresses = objectConverter.fromFile(RowAddresses.class, filesIdPath + rowIdFileNumber);
                    if (rowAddresses != null) {
                        variables.cachedRowAddressesMap.put(rowIdFileNumber, rowAddresses);
                    }
                }
            } finally {
                rowIdLock.unlock(rowIdFileNumber);
            }
        }
        return rowAddresses;
    }

    private synchronized void saveAndClearMap() {
        if (variables.cachedRowAddressesMap.isEmpty()) {
            return;
        }
        final Set<Map.Entry<Integer, RowAddresses>> entrySet = variables.cachedRowAddressesMap.entrySet();
        for (Iterator<Map.Entry<Integer, RowAddresses>> iterator = entrySet.iterator(); iterator.hasNext(); ) {
            final Map.Entry<Integer, RowAddresses> entry = iterator.next();
            LockService.doInLock(rowIdReadWriteLock.writeLock(), entry.getKey(), () -> {
                final RowAddresses rowAddresses = entry.getValue();
                final String rowIdFileName = filesIdPath + entry.getKey();
                if (rowAddresses.baskets.isEmpty()) {
                    variables.idBatches.remove(entry.getKey());
                    variables.cachedRowAddressesMap.remove(entry.getKey());
                    if (!new File(rowIdFileName).delete()) {
                        throw new RuntimeException("cannot delete file " + rowIdFileName);
                    }
                } else if (changed) {
                    objectConverter.toFile(entry.getValue(), rowIdFileName);
                }
                if (entrySet.size() > 1 || rowAddresses.baskets.isEmpty()) {
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
        private final Map<Integer, RowAddresses> cachedRowAddressesMap = new ConcurrentHashMap<>();

        public Variables(AtomicInteger lastId, Set<Integer> idBatches) {
            this.lastId = lastId;
            this.idBatches = idBatches;
        }
    }

    public static class RowAddresses implements Serializable {
        private static final long serialVersionUID = 554239746175423526L;
        public final Map<Integer, RowAddressBasket> baskets = new ConcurrentHashMap<>();
    }

    public static class RowAddressBasket implements Serializable {
        private static final long serialVersionUID = 6741511503259730900L;
        public final Map<Integer, RowAddress> rowAddressMap = new ConcurrentHashMap<>();
        public volatile RowAddress lastRowAddress;

        public RowAddressBasket() {

        }

        public RowAddressBasket(RowAddress lastRowAddress) {
            this.lastRowAddress = lastRowAddress;
        }
    }

    private class BatchStream extends BaseStoppableBatchStream<RowAddress> {
        private final boolean full;
        private final Set<Integer> idSet;
        private final Lock<Integer> lock;

        private BatchStream(boolean full) {
            this.full = full;
            this.idSet = null;
            this.lock = rowReadWriteLock.readLock();
        }

        private BatchStream(Set<Integer> idSet, ProcessType processType) {
            this.full = false;
            this.idSet = idSet;
            this.lock = processType == ProcessType.Write ? rowReadWriteLock.writeLock() : rowReadWriteLock.readLock();
        }

        @Override
        public void forEach(Consumer<RowAddress> consumer) {
            if (full) {
                forEach(variables.idBatches, (rowIdFileNumber, map) -> map, (rowIdFileNumber, rowFileNumber, basket) -> stream(basket, consumer, stopChecker));
            } else {
                if (idSet == null) {
                    return;
                }
                final Map<Integer, Map<Integer, Set<Integer>>> basketMap = makeBaskets(idSet);
                forEach(basketMap.keySet(), (rowIdFileNumber, map) -> {
                    final Map<Integer, Set<Integer>> basketIdes = basketMap.get(rowIdFileNumber);
                    if (basketIdes == null) {
                        return Collections.emptyMap();
                    }
                    final Map<Integer, RowAddressBasket> result = new HashMap<>();
                    basketIdes.keySet().forEach(key -> {
                        final RowAddressBasket basket = map.get(key);
                        if (basket == null) {
                            return;
                        }
                        result.put(key, basket);
                    });
                    return result;
                }, (rowIdFileNumber, rowFileNumber, basket) -> sortedSet(basketMap.get(rowIdFileNumber).get(rowFileNumber)).forEach(id -> process(basket, id, true, consumer)));
            }
        }

        private Map<Integer, Map<Integer, Set<Integer>>> makeBaskets(Set<Integer> idSet) {
            final Map<Integer, Map<Integer, Set<Integer>>> map = new LinkedHashMap<>();
            idSet.forEach(id -> map.computeIfAbsent(getRowIdFileNumber(id), k -> new HashMap<>()).computeIfAbsent(getRowFileNumber(id), k -> new LinkedHashSet<>()).add(id));
            return map;
        }

        private void forEach(Set<Integer> batches, BiFunction<Integer, Map<Integer, RowAddressBasket>, Map<Integer, RowAddressBasket>> mapFunction, TripleConsumer<Integer, Integer, RowAddressBasket> consumer) {
            if (batches == null) {
                return;
            }
            final Integer[] rowIdFileNumber = {null};
            final Integer[] rowFileNumber = {null};
            try (ChainedLock<Integer> rowIdChainedLock = new ChainedLock<>(rowIdReadWriteLock.readLock());
                 ChainedLock<Integer> rowChainedLock = new ChainedLock<>(lock)) {
                for (int value : sortedSet(batches)) {
                    if (stopChecker.get()) {
                        return;
                    }
                    Utils.compareAndRun(value, rowIdFileNumber[0], actual -> {
                        rowIdFileNumber[0] = actual;
                        rowIdChainedLock.init(rowIdFileNumber[0]);
                    });
                    final RowAddresses rowAddresses = getRowAddresses(rowIdFileNumber[0]);
                    if (rowAddresses == null) {
                        continue;
                    }
                    for (Map.Entry<Integer, RowAddressBasket> entry : mapFunction.apply(value, rowAddresses.baskets).entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey)).collect(Collectors.toList())) {
                        if (stopChecker.get()) {
                            return;
                        }
                        Utils.compareAndRun(entry.getKey(), rowFileNumber[0], actual -> {
                            if (rowFileNumber[0] != null) {
                                onBatchEnd.forEach(Runnable::run);
                            }
                            rowFileNumber[0] = actual;
                            rowChainedLock.init(rowFileNumber[0]);
                        });
                        consumer.accept(value, entry.getKey(), entry.getValue());
                    }
                    if (rowFileNumber[0] != null) {
                        onBatchEnd.forEach(Runnable::run);
                    }
                }
                if (rowFileNumber[0] != null) {
                    onStreamEnd.forEach(Runnable::run);
                }
            }
        }

        private void stream(RowAddressBasket basket, Consumer<RowAddress> rowAddressConsumer, AtomicBoolean stopChecker) {
            for (Map.Entry<Integer, RowAddress> entry : basket.rowAddressMap.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey))
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

    interface TripleConsumer<T, U, V> {
        void accept(T t, U u, V v);
    }
}