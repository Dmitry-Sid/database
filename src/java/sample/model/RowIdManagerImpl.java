package sample.model;

import sample.model.pojo.RowAddress;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class RowIdManagerImpl implements RowIdManager {
    private final ObjectConverter objectConverter;
    private final Variables variables;
    private final int maxSize;
    private final String filesPath;

    public RowIdManagerImpl(ObjectConverter objectConverter, int maxSize, String variablesFileName, String filesPath) {
        this.objectConverter = objectConverter;
        this.maxSize = maxSize;
        this.filesPath = filesPath;
        this.variables = objectConverter.fromFile(Variables.class, variablesFileName);
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
            RowAddress rowAddress;
            synchronized (variables.cachedReference) {
                rowAddress = variables.cachedReference.get().rowAddressMap.get(id);
                if (rowAddress == null && !fileName.equals(variables.cachedReference.get().fileName)) {
                    variables.cachedReference.set(new CachedRowAddressMap(fileName, getRowAddressesFromFile(fileName)));
                    rowAddress = variables.cachedReference.get().rowAddressMap.get(id);
                }
                if (rowAddress == null) {
                    return false;
                }
            }
            LockKeeper.getRowIdLock().lock(rowAddress.getId());
            try {
                rowAddressConsumer.accept(rowAddress);
            } finally {
                LockKeeper.getRowIdLock().unlock(rowAddress.getId());
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
        synchronized (variables.cachedReference) {
            if (variables.cachedReference.get().rowAddressMap.containsKey(rowAddress)) {
                contains = true;
            }
        }
        if (contains) {
            synchronized (variables.cachedReference) {
                transformAndRewrite(rowAddress, sizeBefore, sizeAfter, variables.cachedReference.get().rowAddressMap);
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
        return new IdBounds(lowBound, lowBound + maxSize - 1);
    }

    private Map<Integer, RowAddress> getRowAddressesFromFile(String fileName) {
        return objectConverter.fromFile(variables.cachedReference.get().rowAddressMap.getClass(), fileName);
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
        private final AtomicReference<CachedRowAddressMap> cachedReference;

        public Variables(AtomicInteger lastId, Map<IdBounds, String> idBatchMap, AtomicReference<CachedRowAddressMap> cachedReference) {
            this.lastId = lastId;
            this.idBatchMap = idBatchMap;
            this.cachedReference = cachedReference;
        }
    }

    public static class CachedRowAddressMap implements Serializable {
        private static final long serialVersionUID = 6741511503259730900L;
        private final String fileName;
        private final Map<Integer, RowAddress> rowAddressMap;

        public CachedRowAddressMap(String fileName, Map<Integer, RowAddress> rowAddressMap) {
            this.fileName = fileName;
            this.rowAddressMap = rowAddressMap;
        }
    }
}
