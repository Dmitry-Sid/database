package server.model.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.model.*;
import server.model.lock.LockService;
import server.model.lock.ReadWriteLock;
import server.model.pojo.ICondition;
import server.model.pojo.Row;
import server.model.pojo.RowAddress;
import server.model.pojo.TableType;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RowRepositoryImpl extends BaseDestroyable implements RowRepository {
    private static final Logger log = LoggerFactory.getLogger(RowRepositoryImpl.class);
    protected final RowIdRepository rowIdRepository;
    private final ReadWriteLock<String> rowReadWriteLock = LockService.getFileReadWriteLock();
    private final FileHelper fileHelper;
    private final IndexService indexService;
    private final ConditionService conditionService;
    private final ModelService modelService;
    private final Buffer<Row> buffer;
    private final Set<String> fields = Collections.synchronizedSet(new HashSet<>());

    public RowRepositoryImpl(String filePath, boolean init, ObjectConverter objectConverter, DestroyService destroyService, RowIdRepository rowIdRepository, FileHelper fileHelper, IndexService indexService, ConditionService conditionService, ModelService modelService, int bufferSize) {
        super(filePath, init, objectConverter, destroyService);
        this.rowIdRepository = rowIdRepository;
        this.fileHelper = fileHelper;
        this.indexService = indexService;
        this.conditionService = conditionService;
        this.buffer = new BufferImpl<>(bufferSize, bufferConsumer());
        this.modelService = modelService;
        this.fields.addAll(modelService.getFields().stream().map(ModelService.FieldInfo::getName).collect(Collectors.toSet()));
        modelService.subscribeOnFieldsChanges(fields -> {
            synchronized (RowRepositoryImpl.this) {
                processDeletedFields(RowRepositoryImpl.this.fields.stream().filter(field -> !fields.contains(field)).collect(Collectors.toSet()));
                RowRepositoryImpl.this.fields.clear();
                RowRepositoryImpl.this.fields.addAll(fields);
            }
        });
        indexService.subscribeOnNewIndexes(this::processIndexesChanges);
    }

    @Override
    public void add(Row row) {
        boolean added = false;
        if (row.getId() == 0) {
            rowIdRepository.add(rowIdRepository.newId(), rowAddress -> row.setId(rowAddress.getId()));
            added = true;
        }
        if (added) {
            indexService.insert(row);
        } else {
            process(row.getId(), oldRow -> indexService.transform(oldRow, row));
        }
        buffer.add(row, added ? Buffer.State.ADDED : Buffer.State.UPDATED);
    }

    @Override
    public int size(ICondition iCondition, int maxSize) {
        final AtomicInteger size = new AtomicInteger();
        final StoppableStream<Row> stream = stream(iCondition, 0, maxSize);
        stream.forEach(row -> {
            if (maxSize > 0 && size.get() >= maxSize) {
                stream.stop();
                return;
            }
            size.incrementAndGet();
        });
        return size.get();
    }

    private StoppableStream<Row> stream(ICondition iCondition, int from, int size) {
        final IndexService.SearchResult searchResult = indexService.search(iCondition, size);
        final Set<Integer> idSet = searchResult.found ?
                searchResult.idSet.stream().sorted(Integer::compareTo).collect(Collectors.toCollection(LinkedHashSet::new)) : null;
        return new BaseStoppableStream<Row>() {
            private StoppableBatchStream<RowAddress> rowAddressStream;
            private StoppableStream<Buffer.Element<Row>> bufferStream;

            @Override
            public void forEach(Consumer<Row> consumer) {
                final Set<Integer> processedIdSet = new HashSet<>();
                final AtomicInteger skipped = new AtomicInteger();
                final Consumer<Row> rowConsumer = row -> {
                    if (conditionService.check(row, iCondition)) {
                        if (skipped.get() == from) {
                            consumer.accept(row);
                            processedIdSet.add(row.getId());
                        } else {
                            skipped.getAndIncrement();
                        }
                    }
                };
                if (searchResult.found) {
                    for (Iterator<Integer> iterator = idSet.iterator(); iterator.hasNext(); ) {
                        if (stopChecker.get()) {
                            return;
                        }
                        final Integer id = iterator.next();
                        final Buffer.Element<Row> rowElement = buffer.get(id);
                        if (rowElement != null) {
                            if (!Buffer.State.DELETED.equals(rowElement.getState())) {
                                rowConsumer.accept(objectConverter.clone(rowElement.getValue()));
                            }
                            iterator.remove();
                        }
                    }
                }
                if (searchResult.found && idSet.isEmpty()) {
                    return;
                }
                rowAddressStream = searchResult.found ?
                        rowIdRepository.batchStream(idSet, RowIdRepository.StreamType.Read) : rowIdRepository.batchStream();
                try (final FileHelper.ChainStream<InputStream> chainInputStream = fileHelper.getChainInputStream()) {
                    rowAddressStream.addOnBatchEnd(() -> {
                        try {
                            chainInputStream.close();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    rowAddressStream.forEach(processRow(chainInputStream, rowConsumer));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if (stopChecker.get()) {
                    return;
                }
                if (searchResult.found) {
                    return;
                }
                bufferStream = buffer.stream();
                bufferStream.forEach(rowElement -> {
                    final Row row = rowElement.getValue();
                    if (Buffer.State.ADDED.equals(rowElement.getState()) && !processedIdSet.contains(row.getId()) && (conditionService.check(row, iCondition))) {
                        consumer.accept(row);
                    }
                });
            }

            @Override
            public void stop() {
                if (rowAddressStream != null) {
                    rowAddressStream.stop();
                }
                if (bufferStream != null) {
                    bufferStream.stop();
                }
                super.stop();
            }
        };
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
        final Buffer.Element<Row> element = buffer.get(id);
        if (element != null) {
            if (Buffer.State.DELETED.equals(element.getState())) {
                return false;
            }
            consumer.accept(objectConverter.clone(element.getValue()));
            return true;
        }
        return processRowAddress(id, rowAddress -> {
            final byte[] bytes = fileHelper.read(rowAddress);
            if (bytes == null) {
                buffer.add(new Row(id, null), Buffer.State.DELETED);
                return;
            }
            final Row row = objectConverter.fromBytes(Row.class, bytes);
            buffer.add(row, Buffer.State.READ);
            consumer.accept(row);
        });
    }

    private boolean processRowAddress(int id, Consumer<RowAddress> consumer) {
        return rowIdRepository.process(id, rowAddress -> LockService.doInLock(rowReadWriteLock.readLock(), rowIdRepository.getRowFileName(id), () -> consumer.accept(rowAddress)));
    }

    @Override
    public List<Row> getList(ICondition iCondition, int from, int size) {
        if (from < 0) {
            throw new IllegalArgumentException("from cannot be negative");
        }
        if (size < 0) {
            throw new IllegalArgumentException("size cannot be negative");
        }
        if (size == 0) {
            return Collections.emptyList();
        }
        final List<Row> rows = new ArrayList<>();
        final StoppableStream<Row> stream = stream(iCondition, from, size);
        stream.forEach(row -> {
            if (rows.size() >= size) {
                stream.stop();
                return;
            }
            rows.add(row);
        });
        rows.sort(Comparator.comparingInt(TableType::getId));
        return rows;
    }

    private Consumer<RowAddress> processRow(FileHelper.ChainStream<InputStream> chainInputStream, Consumer<Row> rowConsumer) {
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
                Utils.compareAndRun(rowAddress.getFilePath(), fileName.get(), () -> {
                    lastPosition.set(0);
                    fileName.set(rowAddress.getFilePath());
                    chainInputStream.init(fileName.get());
                });
                fileHelper.skip(chainInputStream.getStream(), rowAddress.getPosition() - lastPosition.get());
                lastPosition.set(rowAddress.getPosition() + rowAddress.getSize());
                final byte[] bytes = new byte[rowAddress.getSize()];
                final int actual = chainInputStream.getStream().read(bytes);
                if (actual != bytes.length) {
                    throw new RuntimeException("actual bytes size " + actual + " is not equal " + bytes.length);
                }
                final Row row = objectConverter.fromBytes(Row.class, bytes);
                buffer.add(row, Buffer.State.READ);
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
        final AtomicLong counter = new AtomicLong();
        log.info("processing deleted fields to rows");
        try (final FileHelper.ChainStream<InputStream> chainInputStream = fileHelper.getChainInputStream()) {
            final Consumer<RowAddress> rowAddressConsumer = processRow(chainInputStream, row -> {
                final boolean[] deleted = {false};
                deletedFields.forEach(field -> {
                    if (!deleted[0] && row.getFields().containsKey(field)) {
                        deleted[0] = true;
                    }
                    row.getFields().remove(field);
                });
                if (deleted[0]) {
                    add(row);
                }
                if (deleted[0] && counter.incrementAndGet() % 1000 == 0) {
                    log.info("processed deleted fields " + counter.get() + " rows");
                }
            });
            final StoppableBatchStream<RowAddress> stream = rowIdRepository.batchStream();
            stream.addOnBatchEnd(() -> {
                try {
                    chainInputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            stream.forEach(rowAddressConsumer);
            log.info("processing deleted fields to rows done, count " + counter.get());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void processIndexesChanges(Set<String> indexes) {
        final AtomicLong counter = new AtomicLong();
        log.info("processing inserted indexes to rows");
        try (final FileHelper.ChainStream<InputStream> chainInputStream = fileHelper.getChainInputStream()) {
            final Consumer<RowAddress> rowAddressConsumer = processRow(chainInputStream, row -> {
                indexService.insert(row, indexes);
                if (counter.incrementAndGet() % 1000 == 0) {
                    log.info("processed inserted indexes " + counter.get() + " rows");
                }
            });
            final StoppableBatchStream<RowAddress> stream = rowIdRepository.batchStream();
            stream.addOnBatchEnd(() -> {
                try {
                    chainInputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            stream.forEach(rowAddressConsumer);
            log.info("processing inserted indexes to rows done, count " + counter.get());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Consumer<List<Buffer.Element<Row>>> bufferConsumer() {
        return list -> {
            if (list.isEmpty()) {
                return;
            }
            final Map<Integer, Buffer.Element<Row>> map = list.stream().collect(Collectors.toMap(element -> element.getValue().getId(), Function.identity()));
            final List<Runnable> afterBatchActions = new ArrayList<>();
            final StoppableBatchStream<RowAddress> stream = rowIdRepository.batchStream(map.keySet(), RowIdRepository.StreamType.Write);
            stream.addOnBatchEnd(() -> {
                afterBatchActions.forEach(Runnable::run);
                afterBatchActions.clear();
            });
            fileHelper.collect(stream, collectBean -> {
                final Buffer.Element<Row> element = map.get(collectBean.rowAddress.getId());
                final Row row = element.getValue();
                final byte[] rowBytes = objectConverter.toBytes(row);
                switch (element.getState()) {
                    case ADDED:
                        try {
                            collectBean.outputStream.write(rowBytes);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        afterBatchActions.add(() -> rowIdRepository.save(collectBean.rowAddress.getId(), rowAddress -> rowAddress.setSize(rowBytes.length)));
                        break;
                    case UPDATED:
                        fileHelper.skip(collectBean.inputStream, collectBean.rowAddress.getSize());
                        try {
                            collectBean.outputStream.write(rowBytes);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        afterBatchActions.add(() -> rowIdRepository.save(row.getId(), rowAddressToSave -> rowAddressToSave.setSize(rowBytes.length)));
                        break;
                    case DELETED:
                        fileHelper.skip(collectBean.inputStream, collectBean.rowAddress.getSize());
                        afterBatchActions.add(() -> rowIdRepository.delete(row.getId()));
                        break;
                }
            });
        };
    }

    @Override
    public void destroy() {
        buffer.flush();
    }

    @Override
    public void stop() {
        super.stop();
        rowIdRepository.stop();
        indexService.stop();
        modelService.stop();
    }
}