package server.model.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.model.*;
import server.model.pojo.ICondition;
import server.model.pojo.Row;
import server.model.pojo.RowAddress;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class RowRepositoryImpl implements RowRepository {
    private static final Logger log = LoggerFactory.getLogger(RowRepositoryImpl.class);
    protected final RowIdRepository rowIdRepository;
    private final ObjectConverter objectConverter;
    private final FileHelper fileHelper;
    private final IndexService indexService;
    private final ConditionService conditionService;
    private final Buffer<Row> buffer;
    private final Set<String> fields = Collections.synchronizedSet(new HashSet<>());
    private final ProducerConsumer<Runnable> producerConsumer = new ProducerConsumerImpl<>(1000);
    private volatile boolean destroyed;

    public RowRepositoryImpl(ObjectConverter objectConverter, RowIdRepository rowIdRepository, FileHelper fileHelper, IndexService indexService, ConditionService conditionService, ModelService modelService, int bufferSize, long sleepTime) {
        this.objectConverter = objectConverter;
        this.rowIdRepository = rowIdRepository;
        this.fileHelper = fileHelper;
        this.indexService = indexService;
        this.conditionService = conditionService;
        this.buffer = new BufferImpl<>(bufferSize, bufferConsumer());
        this.fields.addAll(modelService.getFields().stream().map(ModelService.FieldInfo::getName).collect(Collectors.toSet()));
        modelService.subscribeOnFieldsChanges(fields -> processDeletedFields(RowRepositoryImpl.this.fields.stream().filter(field -> !fields.contains(field)).collect(Collectors.toSet())));
        indexService.subscribeOnIndexesChanges(this::processIndexesChanges);
        new Thread(() -> {
            while (!destroyed && !Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (destroyed || Thread.currentThread().isInterrupted()) {
                    break;
                }
                producerConsumer.put(buffer::flush);
            }
        }).start();
        new Thread(() -> {
            while (true) {
                producerConsumer.take().run();
            }
        }).start();
    }

    @Override
    public void add(Row row) {
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
    }

    @Override
    public int size(ICondition iCondition) {
        final Set<Integer> processedIdSet = new HashSet<>();
        final AtomicBoolean stopChecker = new AtomicBoolean(false);
        try (final FileHelper.ChainStream<InputStream> chainInputStream = fileHelper.getChainInputStream()) {
            final Consumer<RowAddress> rowAddressConsumer = processRow(chainInputStream, row -> {
                if (conditionService.check(row, iCondition)) {
                    processedIdSet.add(row.getId());
                }
            });
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
        return rowIdRepository.process(id, rowAddress -> {
            final byte[] bytes = fileHelper.read(rowAddress);
            if (bytes == null) {
                buffer.add(new Row(id, null), Buffer.State.DELETED);
                return;
            }
            consumer.accept(objectConverter.fromBytes(Row.class, bytes));
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
        final Set<Integer> processedIdSet = new HashSet<>();
        final AtomicBoolean stopChecker = new AtomicBoolean(false);
        final AtomicInteger skipped = new AtomicInteger();
        try (final FileHelper.ChainStream<InputStream> chainInputStream = fileHelper.getChainInputStream()) {
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
            });
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
                if (fileName.get() == null) {
                    fileName.set(rowAddress.getFilePath());
                    chainInputStream.init(fileName.get());
                } else if (!fileName.get().equals(rowAddress.getFilePath())) {
                    lastPosition.set(0);
                    fileName.set(rowAddress.getFilePath());
                    chainInputStream.init(fileName.get());
                }
                fileHelper.skip(chainInputStream.getStream(), rowAddress.getPosition() - lastPosition.get());
                lastPosition.set(rowAddress.getPosition() + rowAddress.getSize());
                final byte[] bytes = new byte[rowAddress.getSize()];
                chainInputStream.getStream().read(bytes);
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
        final AtomicLong counter = new AtomicLong();
        log.info("processing deleted fields to rows");
        try (final FileHelper.ChainStream<InputStream> chainInputStream = fileHelper.getChainInputStream()) {
            final Consumer<RowAddress> rowAddressConsumer = processRow(chainInputStream, row -> {
                deletedFields.forEach(field -> row.getFields().remove(field));
                add(row);
                if (counter.incrementAndGet() % 1000 == 0) {
                    log.info("processed deleted fields " + counter.get() + " rows");
                }
            });
            rowIdRepository.stream(rowAddressConsumer, stopChecker, null);
            log.info("processing deleted fields to rows done, count " + counter.get());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void processIndexesChanges() {
        final AtomicBoolean stopChecker = new AtomicBoolean(false);
        final AtomicLong counter = new AtomicLong();
        log.info("processing inserted indexes to rows");
        try (final FileHelper.ChainStream<InputStream> chainInputStream = fileHelper.getChainInputStream()) {
            final Consumer<RowAddress> rowAddressConsumer = processRow(chainInputStream, row -> {
                indexService.insert(row);
                if (counter.incrementAndGet() % 1000 == 0) {
                    log.info("processed inserted indexes " + counter.get() + " rows");
                }
            });
            rowIdRepository.stream(rowAddressConsumer, stopChecker, null);
            log.info("processing inserted indexes to rows done, count " + counter.get());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

    @Override
    public void destroy() {
        producerConsumer.put(buffer::flush);
        destroyed = true;
    }
}