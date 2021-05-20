package server.model.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.model.ObjectConverter;
import server.model.ProducerConsumer;
import server.model.RowIdRepository;
import server.model.lock.Lock;
import server.model.lock.LockService;
import server.model.lock.ReadWriteLock;
import server.model.pojo.RowAddress;

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
    private final ReadWriteLock<String> rowReadWriteLock = LockService.getFileReadWriteLock();
    private final ReadWriteLock<String> rowIdReadWriteLock = LockService.createReadWriteLock(String.class);
    private final Lock<String> rowIdLock = LockService.createLock(String.class);
    private final ProducerConsumer<Runnable> producerConsumer = new ProducerConsumerImpl<>(1000);
    private final ObjectConverter objectConverter;
    private final Variables variables;
    private final String variablesFileName;
    private final String filesIdPath;
    private final String filesRowPath;
    private final int maxIdSize;
    private final int compressSize;
    private volatile boolean destroyed;

    public RowIdRepositoryImpl(ObjectConverter objectConverter, String variablesFileName, String filesIdPath, String filesRowPath, int maxIdSize, int compressSize) {
        this.objectConverter = objectConverter;
        this.filesRowPath = filesRowPath;
        this.compressSize = compressSize;
        this.variablesFileName = variablesFileName;
        if (new File(variablesFileName).exists()) {
            this.variables = objectConverter.fromFile(Variables.class, variablesFileName);
        } else {
            this.variables = new Variables(new AtomicInteger(0), Collections.synchronizedSet(new LinkedHashSet<>()), new ConcurrentHashMap<>());
        }
        this.filesIdPath = filesIdPath;
        this.maxIdSize = maxIdSize;
        new Thread(() -> {
            while (!destroyed && !Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (destroyed || Thread.currentThread().isInterrupted()) {
                    break;
                }
                producerConsumer.put(this::saveAndClearMap);
            }
        }).start();
        new Thread(() -> {
            while (true) {
                producerConsumer.take().run();
            }
        }).start();
    }

    @Override
    public int newId() {
        return variables.lastId.incrementAndGet();
    }

    @Override
    public void add(int id, Consumer<RowAddress> rowAddressConsumer) {
        final String fileName = getRowIdFileName(id);
        final boolean created = !variables.idBatches.contains(getRowIdFileNumber(id));
        if (created) {
            LockService.doInLock(rowIdReadWriteLock.writeLock(), fileName, () -> {
                variables.idBatches.add(getRowIdFileNumber(id));
                final Map<Integer, RowAddress> rowAddressMap = emptyRowAddressMap();
                final RowAddress rowAddress = createRowAddress(id);
                rowAddressConsumer.accept(rowAddress);
                rowAddressMap.put(id, rowAddress);
                variables.cachedRowAddressesMap.put(fileName, new CachedRowAddresses(rowAddressMap, rowAddress));
            });
        } else {
            processRowAddresses(fileName, cachedRowAddresses -> {
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
                    rowAddress = rowAddressFromMap;
                    final int sizeBefore = rowAddress.getSize();
                    rowAddressConsumer.accept(rowAddress);
                    transform(rowAddress.getId(), sizeBefore, rowAddress.getSize());
                }
                cachedRowAddresses.rowAddressMap.put(rowAddress.getId(), rowAddress);
            });
        }
    }

    private RowAddress createRowAddress(int id) {
        return new RowAddress(getRowFileName(id), id, 0, 0);
    }

    private void transform(int id, int sizeBefore, int sizeAfter) {
        if (sizeBefore == sizeAfter) {
            return;
        }
        processRowAddresses(getRowIdFileName(id), cachedRowAddresses -> {
            RowAddress rowAddressNext = cachedRowAddresses.rowAddressMap.get(id);
            if (rowAddressNext == null) {
                throw new RuntimeException("cannot find rowAddress with id : " + id);
            }
            rowAddressNext.setSize(sizeAfter);
            while ((rowAddressNext = cachedRowAddresses.rowAddressMap.get(rowAddressNext.getNext())) != null) {
                rowAddressNext.setPosition(rowAddressNext.getPosition() + (sizeAfter - sizeBefore));
            }
        });
    }

    @Override
    public void stream(Consumer<RowAddress> rowAddressConsumer, AtomicBoolean stopChecker, Set<Integer> idSet) {
        if (idSet == null) {
            for (Integer value : variables.idBatches) {
                for (int i = 1; i <= compressSize; i++) {
                    rowReadWriteLock.readLock().lock(filesRowPath + (compressSize * (value - 1) + i));
                }
                try {
                    if (stopChecker != null && stopChecker.get()) {
                        return;
                    }
                    processRowAddresses(filesIdPath + value, cachedRowAddresses -> stream(cachedRowAddresses.rowAddressMap, rowAddressConsumer, stopChecker));
                } finally {
                    for (int i = 1; i <= compressSize; i++) {
                        rowReadWriteLock.readLock().unlock(filesRowPath + (compressSize * (value - 1) + i));
                    }
                }
            }
        } else {
            for (Integer id : idSet.stream().sorted(Integer::compareTo)
                    .collect(Collectors.toCollection(LinkedHashSet::new))) {
                process(id, rowAddressConsumer);
            }
        }
    }

    private void stream(Map<Integer, RowAddress> rowAddressMap, Consumer<RowAddress> rowAddressConsumer, AtomicBoolean stopChecker) {
        for (Map.Entry<Integer, RowAddress> entry : rowAddressMap.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey))
                .collect(Collectors.toCollection(LinkedHashSet::new))) {
            if (stopChecker != null && stopChecker.get()) {
                return;
            }
            rowAddressConsumer.accept(objectConverter.clone(entry.getValue()));
        }
    }

    @Override
    public void delete(int id) {
        processRowAddresses(getRowIdFileName(id), cachedRowAddresses -> {
            final RowAddress rowAddress = cachedRowAddresses.rowAddressMap.get(id);
            if (rowAddress == null) {
                log.warn("rowAddress not found, id : " + id);
                return;
            }
            transform(id, rowAddress.getSize(), 0);
            if (cachedRowAddresses.rowAddressMap.get(rowAddress.getPrevious()) != null) {
                cachedRowAddresses.rowAddressMap.get(rowAddress.getPrevious()).setNext(rowAddress.getNext());
            }
            if (cachedRowAddresses.rowAddressMap.get(rowAddress.getNext()) != null) {
                cachedRowAddresses.rowAddressMap.get(rowAddress.getNext()).setPrevious(rowAddress.getPrevious());
            }
            cachedRowAddresses.rowAddressMap.remove(id);
            if (cachedRowAddresses.rowAddressMap.isEmpty()) {
                variables.idBatches.remove(getRowIdFileNumber(id));
            }
        });
    }

    @Override
    public boolean process(int id, Consumer<RowAddress> consumer) {
        return LockService.doInLock(rowReadWriteLock.readLock(), getRowFileName(id), () -> {
            final AtomicBoolean processed = new AtomicBoolean();
            processRowAddresses(getRowIdFileName(id), cachedRowAddresses -> {
                final RowAddress rowAddress = cachedRowAddresses.rowAddressMap.get(id);
                if (rowAddress != null) {
                    consumer.accept(objectConverter.clone(rowAddress));
                    processed.set(true);
                }
            });
            return processed.get();
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

    private void processRowAddresses(String fileName, Consumer<CachedRowAddresses> consumer) {
        LockService.doInLock(rowIdReadWriteLock.readLock(), fileName, () -> {
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
            if (cachedRowAddresses != null) {
                consumer.accept(cachedRowAddresses);
            }
        });
    }

    private void saveAndClearMap() {
        final Set<Map.Entry<String, CachedRowAddresses>> entrySet = variables.cachedRowAddressesMap.entrySet();
        final AtomicInteger mapSize = new AtomicInteger(entrySet.size());
        for (Iterator<Map.Entry<String, CachedRowAddresses>> iterator = entrySet.iterator(); iterator.hasNext(); ) {
            final Map.Entry<String, CachedRowAddresses> entry = iterator.next();
            LockService.doInLock(rowIdReadWriteLock.writeLock(), entry.getKey(), () -> {
                final CachedRowAddresses cachedRowAddresses = entry.getValue();
                if (!cachedRowAddresses.rowAddressMap.isEmpty()) {
                    objectConverter.toFile(entry.getValue(), entry.getKey());
                }
                if (mapSize.get() > 1 || cachedRowAddresses.rowAddressMap.isEmpty()) {
                    iterator.remove();
                }
            });
            mapSize.decrementAndGet();
        }
    }

    @Override
    public void destroy() {
        objectConverter.toFile(variables, variablesFileName);
        producerConsumer.put(this::saveAndClearMap);
        destroyed = true;
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
}
