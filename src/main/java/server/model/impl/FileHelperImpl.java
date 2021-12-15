package server.model.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.model.ChainedLock;
import server.model.FileHelper;
import server.model.StoppableBatchStream;
import server.model.Utils;
import server.model.lock.Lock;
import server.model.lock.LockService;
import server.model.lock.ReadWriteLock;
import server.model.pojo.RowAddress;

import java.io.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

public class FileHelperImpl implements FileHelper {
    private static final Logger log = LoggerFactory.getLogger(FileHelperImpl.class);
    private final ReadWriteLock<String> readWriteLock = LockService.getFileReadWriteLock();

    @Override
    public void write(String fileName, byte[] bytes, boolean append) {
        LockService.doInLock(readWriteLock.writeLock(), fileName, () -> {
            try (BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(fileName, append))) {
                output.write(bytes);
            } catch (IOException e) {
                throw new RuntimeException();
            }
        });
    }

    @Override
    public byte[] read(RowAddress rowAddress) {
        return LockService.doInLock(readWriteLock.readLock(), rowAddress.getFilePath(), () -> {
            if (!new File(rowAddress.getFilePath()).exists()) {
                return null;
            }
            try (InputStream inputStream = new BufferedInputStream(new FileInputStream(rowAddress.getFilePath()))) {
                skip(inputStream, rowAddress.getPosition());
                final byte[] bytes = new byte[rowAddress.getSize()];
                inputStream.read(bytes);
                return bytes;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void skip(InputStream inputStream, long size) {
        if (size < 0) {
            throw new IllegalArgumentException("size can't be negative");
        }
        if (inputStream == null || size == 0) {
            return;
        }
        try {
            long skipped = inputStream.skip(size);
            if (skipped < size) {
                for (int i = 0; i < size - skipped; i++) {
                    if (inputStream.available() <= 0) {
                        break;
                    }
                    if (inputStream.available() >= size - skipped - i) {
                        inputStream.skip(size - skipped - i);
                        break;
                    }
                    inputStream.read();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ChainStream<InputStream> getChainInputStream() {
        return new ChainLockStream<>(readWriteLock.readLock(), value -> {
            if (!new File(value).exists()) {
                return null;
            }
            try {
                return new BufferedInputStream(new FileInputStream(value));
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public ChainStream<OutputStream> getChainOutputStream() {
        return new ChainLockStream<>(readWriteLock.writeLock(), value -> {
            try {
                return new BufferedOutputStream(new FileOutputStream(value), 10000);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void collect(StoppableBatchStream<RowAddress> stoppableStream, Consumer<CollectBean> consumer) {
        final String[] inputFileName = {null};
        final String[] tempFileName = {null};
        final long[] inputLastPosition = {0};
        try (ChainStream<InputStream> chainInputStream = getChainInputStream();
             ChainStream<OutputStream> chainOutputStream = getChainOutputStream()) {
            log.info("processing saved rows");
            final AtomicLong counter = new AtomicLong();
            final Runnable saveTempFileRunnable = () -> {
                if (!chainInputStream.isClosed()) {
                    try {
                        writeToEnd(chainInputStream, chainOutputStream);
                        chainInputStream.close();
                        chainOutputStream.close();
                        saveTempFile(tempFileName[0], inputFileName[0]);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            stoppableStream.addOnBatchEnd(saveTempFileRunnable);
            stoppableStream.forEach(rowAddress -> {
                try {
                    Utils.compareAndRun(rowAddress.getFilePath(), inputFileName[0], () -> {
                        saveTempFileRunnable.run();
                        inputLastPosition[0] = 0;
                        inputFileName[0] = rowAddress.getFilePath();
                        tempFileName[0] = getTempFile(inputFileName[0]);
                        chainInputStream.init(rowAddress.getFilePath());
                        chainOutputStream.init(getTempFile(rowAddress.getFilePath()));
                    });
                    boolean found = false;
                    if (chainInputStream.getStream() != null) {
                        while (true) {
                            if (inputLastPosition[0] == rowAddress.getPosition()) {
                                consumer.accept(new CollectBean(rowAddress, chainInputStream.getStream(), chainOutputStream.getStream()));
                                inputLastPosition[0] = rowAddress.getPosition() + rowAddress.getSize();
                                found = true;
                                break;
                            }
                            final int bit = chainInputStream.getStream().read();
                            if (bit == -1) {
                                break;
                            }
                            chainOutputStream.getStream().write(bit);
                            inputLastPosition[0]++;
                        }
                    }
                    if (!found) {
                        consumer.accept(new CollectBean(rowAddress, chainInputStream.getStream(), chainOutputStream.getStream()));
                    }
                    if (counter.incrementAndGet() % 1000 == 0) {
                        log.info("processed " + counter.get() + " saved rows");
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            log.info("processing saved rows done, count " + counter.get());
        } catch (IOException e) {
            delete(new File(tempFileName[0]));
            throw new RuntimeException(e);
        }
    }

    private void writeToEnd(ChainStream<InputStream> chainInputStream, ChainStream<OutputStream> chainOutputStream) throws IOException {
        if (chainInputStream.getStream() != null) {
            int bit;
            while ((bit = chainInputStream.getStream().read()) != -1) {
                chainOutputStream.getStream().write(bit);
            }
        }
    }

    private void saveTempFile(String tempFileName, String inputFileName) {
        LockService.doInLock(readWriteLock.writeLock(), inputFileName, () -> {
            if (!replace(new File(tempFileName), new File(inputFileName))) {
                throw new RuntimeException("can't replace files");
            }
        });
    }

    private boolean replace(File fileFrom, File fileTo) {
        if (!fileTo.exists() || delete(fileTo)) {
            if (isFileEmpty(fileFrom)) {
                return delete(fileFrom);
            }
            return rename(fileFrom, fileTo);
        }
        return false;
    }

    private boolean isFileEmpty(File file) {
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            if (br.readLine() == null) {
                return true;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    private boolean delete(File file) {
        if (file == null) {
            return false;
        }
        if (!file.delete()) {
            throw new RuntimeException("cannot delete file : " + file.getAbsolutePath());
        }
        return true;
    }

    private boolean rename(File fileFrom, File fileTo) {
        if (fileTo == null) {
            return false;
        }
        if (!fileFrom.renameTo(fileTo)) {
            throw new RuntimeException("cannot rename file " + fileFrom.getAbsolutePath() + " to " + fileTo.getName());
        }
        return true;
    }

    private String getTempFile(String fileName) {
        return fileName + ".tmp";
    }

    private static class ChainLockStream<T extends Closeable> extends ChainedLock<String> implements ChainStream<T> {
        private final Function<String, T> streamFunction;
        private T currentStream;

        private ChainLockStream(Lock<String> lock, Function<String, T> streamFunction) {
            super(lock);
            this.streamFunction = streamFunction;
        }

        @Override
        protected void initOthers(String value) {
            try {
                currentStream = streamFunction.apply(value);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public T getStream() {
            return currentStream;
        }

        @Override
        public void close() {
            if (currentStream != null) {
                try {
                    currentStream.close();
                    currentStream = null;
                } catch (IOException e) {
                    log.error("error while closing stream", e);
                    throw new RuntimeException(e);
                }
            }
            super.close();
        }
    }
}
