package server.model.impl;

import server.model.*;
import server.model.lock.LockService;
import server.model.pojo.ICondition;
import server.model.pojo.Row;
import server.model.pojo.RowAddress;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class RowRepositoryImpl implements RowRepository {
    private final ObjectConverter objectConverter;
    private final RowIdRepository rowIdRepository;
    private final FileHelper fileHelper;
    private final IndexService indexService;
    private final ConditionService conditionService;
    private final Buffer<Row> buffer;
    private final Set<String> fields = Collections.synchronizedSet(new HashSet<>());
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public RowRepositoryImpl(ObjectConverter objectConverter, RowIdRepository rowIdRepository, FileHelper fileHelper, IndexService indexService, ConditionService conditionService, ModelService modelService, int bufferSize) {
        this.objectConverter = objectConverter;
        this.rowIdRepository = rowIdRepository;
        this.fileHelper = fileHelper;
        this.indexService = indexService;
        this.conditionService = conditionService;
        this.buffer = new BufferImpl<>(bufferSize, runnableConsumer(), bufferConsumer());
        this.fields.addAll(modelService.getFields().stream().map(ModelService.FieldInfo::getName).collect(Collectors.toSet()));
        modelService.subscribeOnFieldsChanges(fields -> {
            processDeletedFields(RowRepositoryImpl.this.fields.stream().filter(field -> !fields.contains(field)).collect(Collectors.toSet()));
        });
        indexService.subscribeOnIndexesChanges(this::processIndexesChanges);
    }

    @Override
    public void add(Row row) {
        LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Read, () -> {
            boolean added = false;
            if (row.getId() == 0) {
                row.setId(rowIdRepository.newId());
                added = true;
            }
            if (added) {
                indexService.insert(row);
            } else {
                process(row.getId(), oldRow -> indexService.transform(oldRow, row));
            }
            buffer.add(row, added ? Buffer.State.ADDED : Buffer.State.UPDATED);
        });
    }

    @Override
    public int size(ICondition iCondition) {
        return LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Read, () -> {
            final Set<Integer> processedIdSet = new HashSet<>();
            final AtomicBoolean stopChecker = new AtomicBoolean(false);
            try (final FileHelper.ChainInputStream chainInputStream = fileHelper.getChainInputStream()) {
                final Consumer<RowAddress> rowAddressConsumer = processRow(chainInputStream, row -> {
                    if (conditionService.check(row, iCondition)) {
                        processedIdSet.add(row.getId());
                    }
                }, null);
                final IndexService.SearchResult searchResult = indexService.search(iCondition);
                if (searchResult.found) {
                    if (searchResult.idSet != null && searchResult.idSet.size() > 0) {
                        rowIdRepository.stream(rowAddressConsumer, stopChecker, searchResult.idSet);
                    }
                } else {
                    rowIdRepository.stream(rowAddressConsumer, stopChecker, null);
                }
                buffer.stream(rowElement -> {
                    final Row row = rowElement.getValue();
                    final boolean inSet = !searchResult.found || (searchResult.idSet != null && searchResult.idSet.contains(row.getId()));
                    if (Buffer.State.ADDED.equals(rowElement.getState()) && !processedIdSet.contains(row.getId()) &&
                            inSet && (conditionService.check(row, iCondition))) {
                        processedIdSet.add(row.getId());
                    }
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return processedIdSet.size();
        });
    }

    @Override
    public void delete(int id) {
        process(id, row -> {
            buffer.add(row, Buffer.State.DELETED);
            indexService.delete(row);
        });
    }

    @Override
    public boolean process(int id, Consumer<Row> consumer) {
        return LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Read, () -> {
            final Buffer.Element<Row> element = buffer.get(id);
            if (element != null) {
                if (Buffer.State.DELETED.equals(element.getState())) {
                    return false;
                }
                consumer.accept(objectConverter.clone(element.getValue()));
                return true;
            }
            return rowIdRepository.process(id, rowAddress -> {
                final byte[] bytes = fileHelper.read(rowAddress);
                if (bytes == null) {
                    buffer.add(new Row(id, null), Buffer.State.DELETED);
                    return;
                }
                consumer.accept(objectConverter.fromBytes(Row.class, fileHelper.read(rowAddress)));
            });
        });
    }

    @Override
    public List<Row> getList(ICondition iCondition, int from, int size) {
        return LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Read, () -> {
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
            final Set<Integer> processedIdSet = new HashSet<>();
            final AtomicBoolean stopChecker = new AtomicBoolean(false);
            final AtomicInteger skipped = new AtomicInteger();
            try (final FileHelper.ChainInputStream chainInputStream = fileHelper.getChainInputStream()) {
                final Consumer<RowAddress> rowAddressConsumer = processRow(chainInputStream, row -> {
                    if (conditionService.check(row, iCondition)) {
                        if (skipped.get() == from && rows.size() != size) {
                            rows.add(row);
                            processedIdSet.add(row.getId());
                        } else {
                            skipped.getAndIncrement();
                        }
                    }
                    if (rows.size() >= size) {
                        stopChecker.set(true);
                    }
                }, null);
                final IndexService.SearchResult searchResult = indexService.search(iCondition);
                if (searchResult.found) {
                    if (searchResult.idSet != null && searchResult.idSet.size() > 0) {
                        rowIdRepository.stream(rowAddressConsumer, stopChecker, searchResult.idSet);
                    }
                } else {
                    rowIdRepository.stream(rowAddressConsumer, stopChecker, null);
                }
                if (rows.size() < size) {
                    buffer.stream(rowElement -> {
                        final Row row = rowElement.getValue();
                        final boolean inSet = !searchResult.found || (searchResult.idSet != null && searchResult.idSet.contains(row.getId()));
                        if (Buffer.State.ADDED.equals(rowElement.getState()) && !processedIdSet.contains(row.getId()) &&
                                rows.size() < size && inSet && (conditionService.check(row, iCondition))) {
                            rows.add(row);
                        }
                    });
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return rows;
        });
    }

    private Consumer<RowAddress> processRow(FileHelper.ChainInputStream chainInputStream, Consumer<Row> rowConsumer, Runnable nextFileAction) {
        final AtomicReference<String> fileName = new AtomicReference<>();
        final AtomicLong lastPosition = new AtomicLong(0);
        return rowAddress -> {
            try {
                final Buffer.Element<Row> rowElement = buffer.get(rowAddress.getId());
                if (rowElement != null) {
                    if (!Buffer.State.DELETED.equals(rowElement.getState())) {
                        rowConsumer.accept(objectConverter.clone(rowElement.getValue()));
                    }
                    return;
                }
                if (fileName.get() == null) {
                    fileName.set(rowAddress.getFilePath());
                    chainInputStream.read(fileName.get());
                } else if (!fileName.get().equals(rowAddress.getFilePath())) {
                    lastPosition.set(0);
                    fileName.set(rowAddress.getFilePath());
                    chainInputStream.read(fileName.get());
                    if (nextFileAction != null) {
                        nextFileAction.run();
                    }
                }
                fileHelper.skip(chainInputStream.getInputStream(), rowAddress.getPosition() - lastPosition.get());
                lastPosition.set(rowAddress.getPosition() + rowAddress.getSize());
                final byte[] bytes = new byte[rowAddress.getSize()];
                chainInputStream.getInputStream().read(bytes);
                final Row row = objectConverter.fromBytes(Row.class, bytes);
                rowConsumer.accept(row);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private void processDeletedFields(Set<String> deletedFields) {
        if (deletedFields.isEmpty()) {
            return;
        }
        final AtomicBoolean stopChecker = new AtomicBoolean(false);
        final List<Row> rows = new ArrayList<>();
        try (final FileHelper.ChainInputStream chainInputStream = fileHelper.getChainInputStream()) {
            final Consumer<RowAddress> rowAddressConsumer = processRow(chainInputStream, row -> {
                deletedFields.forEach(field -> row.getFields().remove(field));
                rows.add(row);
            }, () -> {
                final Iterator<Row> rowIterator = rows.iterator();
                while (rowIterator.hasNext()) {
                    final Row row = rowIterator.next();
                    add(row);
                    rowIterator.remove();
                }
            });
            rowIdRepository.stream(rowAddressConsumer, stopChecker, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        rows.forEach(this::add);
    }

    private void processIndexesChanges() {
        LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Read, () -> {
            final AtomicBoolean stopChecker = new AtomicBoolean(false);
            try (final FileHelper.ChainInputStream chainInputStream = fileHelper.getChainInputStream()) {
                final Consumer<RowAddress> rowAddressConsumer = processRow(chainInputStream, indexService::insert, null);
                rowIdRepository.stream(rowAddressConsumer, stopChecker, null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Consumer<Runnable> runnableConsumer() {
        return runnable -> {
            final int readCount;
            if (readWriteLock instanceof ReentrantReadWriteLock) {
                readCount = ((ReentrantReadWriteLock) readWriteLock).getReadHoldCount();
            } else {
                readCount = 0;
            }
            for (int i = 0; i < readCount; i++) {
                readWriteLock.readLock().unlock();
            }
            LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Write, runnable);
            for (int i = 0; i < readCount; i++) {
                readWriteLock.readLock().lock();
            }
        };
    }

    private Consumer<List<Buffer.Element<Row>>> bufferConsumer() {
        return list -> {
            final List<FileHelper.CollectBean> fileHelperList = new ArrayList<>();
            list.forEach(element -> {
                final Row row = element.getValue();
                switch (element.getState()) {
                    case ADDED:
                        processAdded(row, fileHelperList);
                        break;
                    case UPDATED:
                        boolean processed = rowIdRepository.process(row.getId(), rowAddress -> {
                            final byte[] rowBytes = objectConverter.toBytes(row);
                            fileHelperList.add(new FileHelper.CollectBean(rowAddress, (inputStream, outputStream) -> {
                                fileHelper.skip(inputStream, rowAddress.getSize());
                                outputStream.write(rowBytes);
                            }, () -> rowIdRepository.add(row.getId(), rowAddressToSave -> rowAddressToSave.setSize(rowBytes.length))));
                        });
                        if (!processed) {
                            processAdded(row, fileHelperList);
                        }
                        break;
                    case DELETED:
                        rowIdRepository.process(row.getId(), rowAddress ->
                                fileHelperList.add(new FileHelper.CollectBean(rowAddress,
                                        (inputStream, outputStream) -> fileHelper.skip(inputStream, rowAddress.getSize()),
                                        () -> rowIdRepository.delete(row.getId()))));
                        break;
                }
            });
            fileHelper.collect(fileHelperList);
        };
    }

    private void processAdded(Row row, List<FileHelper.CollectBean> fileHelperList) {
        rowIdRepository.add(row.getId(), rowAddress -> {
            final byte[] rowBytes = objectConverter.toBytes(row);
            rowAddress.setSize(rowBytes.length);
            fileHelperList.add(new FileHelper.CollectBean(rowAddress, (inputStream, outputStream) -> outputStream.write(rowBytes), null));
        });
    }

    private void destroy() {
        buffer.flush();
    }
}