package sample.model;

import sample.model.pojo.RowAddress;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class RowIdManagerImpl implements RowIdManager {
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
    public int newId() {
        synchronized (variables.lastId) {
            return variables.lastId.get() + 1;
        }
    }

    @Override
    public boolean process(int id, Consumer<RowAddress> rowAddressConsumer) {
        final String fileName = getFileName(id);
        if (fileName == null) {
            return false;
        }
        LockKeeper.getFileLock().lock(fileName);
        try {
            final RowAddress rowAddress = cacheAndGetRowAddress(id, fileName);
            if (rowAddress == null) {
                return false;
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
    public void transform(int id, int newSize) {
        if (newSize <= 0) {
            throw new RuntimeException("new size must be positive");
        }
        synchronized (CACHED_LOCK) {
            if (variables.cachedRowAddresses.rowAddressMap.containsKey(id)) {
                doInFileLock(variables.cachedRowAddresses.fileName, () -> transformAndRewrite(id, newSize,
                        variables.cachedRowAddresses.fileName, variables.cachedRowAddresses.rowAddressMap));
                return;
            }
        }
        final String fileName = getFileName(id);
        if (fileName == null) {
            throw new RuntimeException("cannot find rowAddress with id : " + id);
        }
        doInFileLock(fileName, () -> transformAndRewrite(id, newSize, fileName, getRowAddressesFromFile(fileName)));
    }

    private void doInFileLock(String fileName, Runnable runnable) {
        LockKeeper.getFileLock().lock(fileName);
        try {
            runnable.run();
        } finally {
            LockKeeper.getFileLock().unlock(fileName);
        }
    }

    private void transformAndRewrite(int id, int sizeAfter, String fileName, Map<Integer, RowAddress> rowAddressMap) {
        RowAddress rowAddressNext = rowAddressMap.get(id);
        if (rowAddressNext == null) {
            throw new RuntimeException("cannot find rowAddress with id : " + id);
        }
        final int sizeBefore = rowAddressNext.getSize();
        rowAddressNext.setSize(sizeAfter);
        while ((rowAddressNext = rowAddressNext.getNext()) != null) {
            rowAddressNext.setPosition(rowAddressNext.getPosition() + (sizeAfter - sizeBefore));
        }
        objectConverter.toFile((Serializable) rowAddressMap, fileName);
    }

    @Override
    public void add(int id, String rowFileName, Consumer<RowAddress> rowAddressConsumer) {
        if (id <= 0) {
            throw new RuntimeException("new size must be positive");
        }
        LockKeeper.getRowIdLock().lock(id);
        try {
            String fileName;
            boolean created = false;
            synchronized (variables.idBatchMap) {
                fileName = variables.idBatchMap.get(getBounds(id));
                if (fileName == null) {
                    fileName = getNewFileName(id);
                    created = true;
                }
            }
            if (!created) {
                final String finalFileName = fileName;
                doInFileLock(fileName, () -> {
                    synchronized (CACHED_LOCK) {
                        final RowAddress lastRowAddress = cacheAndGetRowAddress(variables.lastId.get(), finalFileName);
                        if (variables.cachedRowAddresses.rowAddressMap.containsKey(id)) {
                            throw new RuntimeException("already has same id : " + id);
                        }
                        LockKeeper.getRowIdLock().lock(lastRowAddress.getId());
                        try {
                            final RowAddress rowAddress = new RowAddress(rowFileName, id, 0, 0);
                            rowAddressConsumer.accept(rowAddress);
                            lastRowAddress.setNext(rowAddress);
                            variables.cachedRowAddresses.rowAddressMap.put(rowAddress.getId(), rowAddress);
                            objectConverter.toFile((Serializable) variables.cachedRowAddresses.rowAddressMap, finalFileName);
                        } finally {
                            LockKeeper.getRowIdLock().unlock(lastRowAddress.getId());
                        }
                    }
                });
            } else {
                final int fileNumber = id / maxSize;
                final RowAddress rowAddress = new RowAddress(rowFileName, id, 0, 0);
                rowAddressConsumer.accept(rowAddress);
                doInFileLock(filesIdPath + fileNumber, () -> {
                    synchronized (variables.idBatchMap) {
                        variables.idBatchMap.put(getBounds(id), filesIdPath + fileNumber);
                    }
                    final Map<Integer, RowAddress> rowAddressMap = emptyRowAddressMap();
                    rowAddressMap.put(id, rowAddress);
                    objectConverter.toFile((Serializable) rowAddressMap, filesIdPath + fileNumber);
                });
            }
            variables.lastId.set(id);
        } finally {
            LockKeeper.getRowIdLock().unlock(id);
        }
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

    private String getNewFileName(int id) {
        return filesIdPath + id / maxSize;
    }

    private String getFileName(int id) {
        synchronized (variables.idBatchMap) {
            return variables.idBatchMap.get(getBounds(id));
        }
    }

    private IdBounds getBounds(int id) {
        final int lowBound = maxSize * (id / maxSize) + 1;
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
