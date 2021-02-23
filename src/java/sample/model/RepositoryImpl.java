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
            if (row.getId() == 0) {
                row.setId(rowIdManager.newId());
            }
            LockKeeper.getRowIdLock().lock(row.getId());
            try {
                final boolean processed = rowIdManager.process(row.getId(),
                        rowAddress -> fileHelper.collectFile(rowAddress, (inputStream, outputStream) -> {
                            final byte[] rowBytes = objectConverter.toBytes(row);
                            inputStream.skip(rowAddress.getSize() - 1);
                            outputStream.write(rowBytes);
                            rowIdManager.transform(rowAddress.getId(), rowBytes.length);
                        }));
                if (!processed) {
                    /*rowIdManager.add(row.getId(), "", rowAddress -> {

                    });*/
                }
                // transformIndexes(row, processed);
            } finally {
                LockKeeper.getRowIdLock().unlock(row.getId());
            }
        }
    }

    @Override
    public void delete(String id) {

    }

    @Override
    public Row get(int id) {
        return null;
    }


    /*private final String fileName;
    private final RowConverter rowConverter;
    private static final byte[] markBytes = "mark".getBytes();
    private Map<String, AppAddress> appAddressMap = new HashMap<>();
    private AppAddress last;

    public RepositoryImpl(String fileName, RowConverter rowConverter) {
        this.fileName = fileName;
        this.rowConverter = rowConverter;
        processFile();
    }

    private void processFile() {
        try (FileInputStream fis = new FileInputStream(fileName)) {
            int markPosition = 0;
            long position = 0;
            int bit;
            boolean startApp = false;
            final List<Byte> bytes = new ArrayList<>();
            while ((bit = fis.read()) != -1) {
                if (markPosition == markBytes.length) {
                    startApp = true;
                    markPosition = 0;
                    if (!bytes.isEmpty()) {
                        last = processApplication(last, position - markBytes.length, fromListToArray(bytes.subList(0, bytes.size() - markBytes.length)));
                    }
                    bytes.clear();
                }
                if (markBytes[markPosition] == bit) {
                    markPosition++;
                } else {
                    markPosition = 0;
                }
                if (startApp) {
                    bytes.add((byte) bit);
                }
                position++;
            }
            if (!bytes.isEmpty()) {
                last = processApplication(last, position, fromListToArray(bytes));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private AppAddress processApplication(AppAddress last, long position, byte[] bytes) {
        final Application application = rowConverter.fromBytes(bytes);
        final AppAddress appAddress = new AppAddress(position - bytes.length, bytes.length);
        appAddressMap.put(application.getId(), appAddress);
        if (last != null) {
            last.next = appAddress;
        }
        return appAddress;
    }

    private byte[] fromListToArray(List<Byte> bytes) {
        final byte[] byteArray = new byte[bytes.size()];
        for (int i = 0; i < bytes.size(); i++) {
            byteArray[i] = bytes.get(i);
        }
        return byteArray;
    }

    @Override
    public void add(Application application) {
        final AppAddress appAddress = appAddressMap.get(application.getId());
        if (appAddress != null) {
            collectFile(appAddress, (inputStream, outputStream) -> {
                final byte[] appBytes = rowConverter.toBytes(application);
                inputStream.skip(appAddress.size - 1);
                outputStream.write(appBytes);
                transformAppMap(appAddress, appAddress.size, appBytes.length);
            });
        } else {
            try (FileOutputStream output = new FileOutputStream(fileName, true)) {
                final byte[] bytes = rowConverter.toBytes(application);
                output.write(markBytes);
                output.write(bytes);
                last = processApplication(last, last.position + last.size + bytes.length + markBytes.length, bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void transformAppMap(AppAddress appAddress, int sizeBefore, int sizeAfter) {
        appAddress.size = sizeAfter;
        AppAddress appAddressNext = appAddress.next;
        while (appAddressNext != null) {
            appAddressNext.position = appAddressNext.position + (sizeAfter - sizeBefore);
            appAddressNext = appAddressNext.next;
        }
    }

    @Override
    public void delete(String id) {
        final AppAddress appAddress = appAddressMap.get(id);
        if (appAddress == null) {
            throw new RuntimeException("unknown id");
        }
        collectFile(appAddress, (inputStream, outputStream) -> {
            inputStream.skip(appAddress.size - 1 + markBytes.length);
            transformAppMap(appAddress, appAddress.size, -markBytes.length);
            appAddressMap.remove(id);
        });
    }





    @Override
    public Application get(String id) {
        final AppAddress appAddress = appAddressMap.get(id);
        if (appAddress == null) {
            return null;
        }
        try (FileInputStream fis = new FileInputStream(fileName)) {
            fis.skip(appAddress.position);
            final byte[] bytes = new byte[appAddress.size];
            fis.read(bytes);
            return rowConverter.fromBytes(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    */
}
