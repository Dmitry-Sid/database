package sample.model;

import sample.model.pojo.ICondition;
import sample.model.pojo.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class RepositoryImpl implements Repository {
    private final ObjectConverter objectConverter;
    private final RowIdManager rowIdManager;
    private final FileHelper fileHelper;
    private final IndexService indexService;
    private final ConditionService conditionService;

    public RepositoryImpl(ObjectConverter objectConverter, RowIdManager rowIdManager, FileHelper fileHelper, IndexService indexService, ConditionService conditionService) {
        this.objectConverter = objectConverter;
        this.rowIdManager = rowIdManager;
        this.fileHelper = fileHelper;
        this.indexService = indexService;
        this.conditionService = conditionService;
    }

    @Override
    public void add(Row row) {
        synchronized (row) {
            boolean processed = false;
            if (row.getId() != 0) {
                processed = LockService.doInRowIdLock(row.getId(), () -> rowIdManager.process(row.getId(),
                        rowAddress -> fileHelper.collect(rowAddress, (inputStream, outputStream) -> {
                            final byte[] rowBytes = objectConverter.toBytes(row);
                            inputStream.skip(rowAddress.getSize() - 1);
                            outputStream.write(rowBytes);
                            rowIdManager.transform(rowAddress.getId(), rowBytes.length);
                        })));
            }
            if (!processed) {
                rowIdManager.add(rowAddress -> {
                    row.setId(rowAddress.getId());
                    final byte[] rowBytes = objectConverter.toBytes(row);
                    rowAddress.setSize(rowBytes.length);
                    fileHelper.write(rowAddress.getFilePath(), rowBytes, true);
                    return true;
                });
            }
            // transformIndexes(row, processed);
        }
    }

    @Override
    public void delete(int id) {
        LockService.doInRowIdLock(id, () -> {
            final boolean processed = rowIdManager.process(id, rowAddress ->
                    fileHelper.collect(rowAddress, (inputStream, outputStream) -> {
                        inputStream.skip(rowAddress.getSize() - 1);
                        rowIdManager.delete(id);
                        // transformIndexes(row, processed);
                    }));
            if (!processed) {
                throw new RuntimeException("unknown id : " + id);
            }
            return null;
        });
    }

    @Override
    public Row get(int id) {
        return LockService.doInRowIdLock(id, () -> {
            final Row[] rows = new Row[1];
            rowIdManager.process(id, rowAddress -> rows[0] = objectConverter.fromBytes(Row.class, fileHelper.read(rowAddress)));
            return rows[0];
        });
    }

    @Override
    public List<Row> getList(ICondition iCondition, int from, int size) {
        if (from < 0) {
            throw new RuntimeException("from cannot be negative");
        }
        if (size < 0) {
            throw new RuntimeException("size cannot be negative");
        }
        if (size == 0) {
            return Collections.emptyList();
        }
        final List<Row> rows = new ArrayList<>();
        final AtomicReference<String> fileName = new AtomicReference<>();
        final AtomicBoolean stopChecker = new AtomicBoolean(false);
        final AtomicInteger skipped = new AtomicInteger();
        final AtomicLong lastPosition = new AtomicLong(0);
        final FileHelper.ChainInputStream chainInputStream = fileHelper.getChainInputStream();
        rowIdManager.process(indexService.search(iCondition), rowAddress -> {
            try {
                if (fileName.get() == null) {
                    fileName.set(rowAddress.getFilePath());
                    chainInputStream.read(rowAddress.getFilePath());
                } else if (!fileName.get().equals(rowAddress.getFilePath())) {
                    lastPosition.set(0);
                    fileName.set(rowAddress.getFilePath());
                    chainInputStream.read(rowAddress.getFilePath());
                }
                if (lastPosition.get() == 0 && rowAddress.getSize() != 0) {
                    chainInputStream.getInputStream().skip(rowAddress.getPosition());
                    lastPosition.set(rowAddress.getPosition() + rowAddress.getSize());
                } else {
                    chainInputStream.getInputStream().skip(rowAddress.getPosition() - lastPosition.get());
                }
                final byte[] bytes = new byte[rowAddress.getSize()];
                chainInputStream.getInputStream().read(bytes);
                final Row row = objectConverter.fromBytes(Row.class, bytes);
                if (conditionService.check(row, iCondition)) {
                    if (skipped.get() == from && rows.size() != size) {
                        rows.add(row);
                    } else {
                        skipped.getAndIncrement();
                    }
                }
                if (rows.size() == size) {
                    stopChecker.set(true);
                }
            } catch (IOException e) {
                chainInputStream.close();
                throw new RuntimeException(e);
            }
        }, stopChecker);
        return rows;
    }
}
