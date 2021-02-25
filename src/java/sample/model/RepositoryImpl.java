package sample.model;

import sample.model.pojo.Row;

public class RepositoryImpl implements Repository {
    private final ObjectConverter objectConverter;
    private final RowIdManager rowIdManager;
    private final FileHelper fileHelper;

    public RepositoryImpl(ObjectConverter objectConverter, RowIdManager rowIdManager, FileHelper fileHelper) {
        this.objectConverter = objectConverter;
        this.rowIdManager = rowIdManager;
        this.fileHelper = fileHelper;
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
}
