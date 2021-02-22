package sample.model;

import sample.model.pojo.RowAddress;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class RowIdManagerImpl implements RowIdManager {
    private final ObjectConverter objectConverter;
    private final Variables variables;
    private final AtomicReference<Map<Integer, RowAddress>> currentRowAddressMap;
    private final int maxSize;

    public RowIdManagerImpl(ObjectConverter objectConverter, Variables variables, AtomicReference<Map<Integer, RowAddress>> currentRowAddressMap, int maxSize) {
        this.objectConverter = objectConverter;
        this.variables = variables;
        if (currentRowAddressMap == null) {
            this.currentRowAddressMap = new AtomicReference<>(new ConcurrentHashMap<>());
        } else {
            this.currentRowAddressMap = currentRowAddressMap;
        }
        this.maxSize = maxSize;
    }

    public RowIdManagerImpl(ObjectConverter objectConverter, int maxSize, String fileName) {
        this.objectConverter = objectConverter;
        this.maxSize = maxSize;
        this.variables = objectConverter.fromFile(Variables.class, fileName);
        this.currentRowAddressMap = new AtomicReference<>(getRowAddressesFromFile(variables.idBatchMap.get(getBounds(variables.lastId.get()))));
    }


    @Override
    public int newId() {
        return variables.lastId.incrementAndGet();
    }

    @Override
    public boolean process(int id, Consumer<RowAddress> rowAddressConsumer) {
        String fileName;
        synchronized (variables.idBatchMap) {
            fileName = variables.idBatchMap.get(getBounds(id));
        }
        if (fileName == null) {
            return false;
        }
        LockKeeper.getFileLock().lock(fileName);
        try {
            RowAddress rowAddress = currentRowAddressMap.get().get(id);
            if (rowAddress == null) {
                synchronized (currentRowAddressMap) {
                    currentRowAddressMap.set(getRowAddressesFromFile(fileName));
                    rowAddress = currentRowAddressMap.get().get(id);
                }
            }
            synchronized (rowAddress) {
                rowAddressConsumer.accept(rowAddress);
            }
        } finally {
            LockKeeper.getFileLock().unlock(fileName);
        }
        return true;
    }

    @Override
    public void transform(RowAddress rowAddress, int sizeBefore, int sizeAfter) {
        rowAddress.setSize(sizeAfter);
        boolean contains = false;
        synchronized (currentRowAddressMap.get()) {
            if (currentRowAddressMap.get().containsKey(rowAddress)) {
                contains = true;
            }
        }
        if (contains) {
            synchronized (currentRowAddressMap.get()) {
                transformAndRewrite(rowAddress, sizeBefore, sizeAfter, currentRowAddressMap.get());
            }
        } else {
            transformAndRewrite(rowAddress, sizeBefore, sizeAfter, getRowAddressesFromFile(rowAddress.getFilePath()));
        }
    }

    private void transformAndRewrite(RowAddress rowAddress, int sizeBefore, int sizeAfter, Map<Integer, RowAddress> rowAddressMap) {
        RowAddress rowAddressNext;
        while ((rowAddressNext = rowAddress.getNext()) != null) {
            rowAddressNext.setPosition(rowAddressNext.getPosition() + (sizeAfter - sizeBefore));
        }
        objectConverter.toFile((Serializable) rowAddressMap, rowAddress.getFilePath());
    }

    @Override
    public boolean add(int id, Consumer<RowAddress> rowAddressConsumer) {
        return false;
    }

    private IdBounds getBounds(int id) {
        final int lowBound = maxSize * (id / maxSize) + 1;
        return new IdBounds(lowBound, lowBound + maxSize);
    }

    private Map<Integer, RowAddress> getRowAddressesFromFile(String fileName) {
        return objectConverter.fromFile(currentRowAddressMap.get().getClass(), fileName);
    }

    private class IdBounds implements Serializable {
        private static final long serialVersionUID = -6498812036951981474L;

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

        private final int lowBound;
        private final int highBound;

        private IdBounds(int lowBound, int highBound) {
            this.lowBound = lowBound;
            this.highBound = highBound;
        }
    }

    private class Variables implements Serializable {
        private static final long serialVersionUID = 1228422981455428546L;
        private final AtomicInteger lastId;
        private final Map<IdBounds, String> idBatchMap;

        private Variables(AtomicInteger lastId, Map<IdBounds, String> idBatchMap) {
            this.lastId = lastId;
            this.idBatchMap = idBatchMap;
        }
    }
}
