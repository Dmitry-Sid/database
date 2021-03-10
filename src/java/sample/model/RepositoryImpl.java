package sample.model;

import sample.model.pojo.ICondition;
import sample.model.pojo.Row;
import sample.model.pojo.RowAddress;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class RepositoryImpl implements Repository {
    private final Set<String> fields = Collections.synchronizedSet(new HashSet<>());
    private final ObjectConverter objectConverter;
    private final RowIdManager rowIdManager;
    private final FileHelper fileHelper;
    private final IndexService indexService;
    private final ConditionService conditionService;

    public RepositoryImpl(ObjectConverter objectConverter, RowIdManager rowIdManager, FileHelper fileHelper, IndexService indexService, ConditionService conditionService, ModelService modelService) {
        this.objectConverter = objectConverter;
        this.rowIdManager = rowIdManager;
        this.fileHelper = fileHelper;
        this.indexService = indexService;
        this.conditionService = conditionService;
        fields.addAll(modelService.getFields());
        modelService.subscribeOnFieldsChanges(fields -> {
            final Set<String> removedFields = RepositoryImpl.this.fields.stream().filter(field -> !fields.contains(field)).collect(Collectors.toSet());

        });
    }

    @Override
    public void add(Row row) {
        synchronized (row) {
            boolean processed = false;
            if (row.getId() != 0) {
                processed = LockService.doInRowIdLock(row.getId(), () -> rowIdManager.process(row.getId(),
                        rowAddress -> fileHelper.collect(rowAddress, (inputStream, outputStream) -> {
                            final byte[] oldRowBytes = new byte[rowAddress.getSize()];
                            inputStream.read(oldRowBytes);
                            final Row oldRow = objectConverter.fromBytes(Row.class, oldRowBytes);
                            final byte[] rowBytes = objectConverter.toBytes(row);
                            outputStream.write(rowBytes);
                            rowIdManager.transform(rowAddress.getId(), rowBytes.length);
                            indexService.transform(oldRow, row);
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
                indexService.insert(row);
            }
        }
    }

    @Override
    public void delete(int id) {
        final boolean processed = LockService.doInRowIdLock(id, () -> rowIdManager.process(id, rowAddress ->
                fileHelper.collect(rowAddress, (inputStream, outputStream) -> {
                    final byte[] oldRowBytes = new byte[rowAddress.getSize()];
                    inputStream.read(oldRowBytes);
                    final Row row = objectConverter.fromBytes(Row.class, oldRowBytes);
                    rowIdManager.delete(id);
                    indexService.delete(row);
                })));
        if (!processed) {
            throw new RuntimeException("unknown id : " + id);
        }
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
        try (final FileHelper.ChainInputStream chainInputStream = fileHelper.getChainInputStream()) {
            final Consumer<RowAddress> rowAddressConsumer = rowAddress -> {
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
                    } else {
                        chainInputStream.getInputStream().skip(rowAddress.getPosition() - lastPosition.get());
                    }
                    lastPosition.set(rowAddress.getPosition() + rowAddress.getSize());
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
                    throw new RuntimeException(e);
                }
            };
            final IndexService.SearchResult searchResult = indexService.search(iCondition);
            if (searchResult.found) {
                rowIdManager.process(searchResult.idSet, rowAddressConsumer, stopChecker);
            } else {
                rowIdManager.stream(rowAddressConsumer, stopChecker);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return rows;
    }
}
