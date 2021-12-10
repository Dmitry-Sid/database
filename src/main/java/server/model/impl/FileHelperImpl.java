package server.model.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.model.ChainedLock;
import server.model.FileHelper;
import server.model.StoppableStream;
import server.model.lock.Lock;
import server.model.lock.LockService;
import server.model.lock.ReadWriteLock;
import server.model.pojo.RowAddress;

import java.io.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class FileHelperImpl implements FileHelper {
    private static final Logger log = LoggerFactory.getLogger(FileHelperImpl.class);
    private final ReadWriteLock<String> readWriteLock = LockService.getFileReadWriteLock();

    @Override
    public void write(String fileName, byte[] bytes, boolean append) {
        LockService.doInLock(readWriteLock.writeLock(), fileName, () -> {
            try (BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(fileName, append))) {
                output.write(bytes);
                output.flush();
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
        return new ChainLockInputStream(readWriteLock.readLock());
    }

    @Override
    public ChainStream<OutputStream> getChainOutputStream() {
        return new ChainLockOutputStream(readWriteLock.writeLock());
    }

    @Override
    public void collect(StoppableStream<RowAddress> stoppableStream, Consumer<CollectBean> consumer) {
        final String[] inputFileName = {null};
        final String[] tempFileName = {null};
        final long[] inputLastPosition = {0};
        try (ChainStream<InputStream> chainInputStream = getChainInputStream();
             ChainStream<OutputStream> chainOutputStream = getChainOutputStream()) {
            log.info("processing saved rows");
            final AtomicLong counter = new AtomicLong();
            stoppableStream.forEach(rowAddress -> {
                try {
                    if (inputFileName[0] == null) {
                        inputFileName[0] = rowAddress.getFilePath();
                        tempFileName[0] = getTempFile(inputFileName[0]);
                        chainInputStream.init(inputFileName[0]);
                        chainOutputStream.init(tempFileName[0]);
                    } else if (!inputFileName[0].equals(rowAddress.getFilePath())) {
                        writeToEnd(chainInputStream, chainOutputStream);
                        inputLastPosition[0] = 0;
                        chainInputStream.init(rowAddress.getFilePath());
                        chainOutputStream.init(getTempFile(rowAddress.getFilePath()));
                        saveTempFile(tempFileName[0], inputFileName[0]);
                        inputFileName[0] = rowAddress.getFilePath();
                        tempFileName[0] = getTempFile(inputFileName[0]);
                    }
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
            }, () -> {
                try {
                    writeToEnd(chainInputStream, chainOutputStream);
                    chainInputStream.close();
                    chainOutputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if (inputFileName[0] != null) {
                    saveTempFile(tempFileName[0], inputFileName[0]);
                }
            });
            log.info("processing saved rows done, count " + counter.get());
        } catch (IOException e) {
            delete(new File(tempFileName[0]));
            throw new RuntimeException(e);
        }
    }

    private void saveTempFile(String tempFileName, String inputFileName) {
        LockService.doInLock(readWriteLock.writeLock(), inputFileName, () -> {
            deleteAndRename(new File(tempFileName), new File(inputFileName));
        });
    }

    private void writeToEnd(ChainStream<InputStream> chainInputStream, ChainStream<OutputStream> chainOutputStream) throws IOException {
        if (chainInputStream.getStream() != null) {
            int bit;
            while ((bit = chainInputStream.getStream().read()) != -1) {
                chainOutputStream.getStream().write(bit);
            }
        }
    }

    private String getTempFile(String fileName) {
        return fileName + ".tmp";
    }

    private boolean deleteAndRename(File fileFrom, File fileTo) {
        if (!fileTo.exists() || delete(fileTo)) {
            return rename(fileFrom, fileTo);
        }
        return false;
    }

    private boolean delete(File file) {
        if (file == null) {
            return false;
        }
        if (!file.delete()) {
            log.warn("cannot delete file : " + file.getAbsolutePath());
            return false;
        }
        return true;
    }

    private boolean rename(File fileFrom, File fileTo) {
        if (fileTo == null) {
            return false;
        }
        if (!fileFrom.renameTo(fileTo)) {
            log.warn("cannot rename file " + fileFrom.getAbsolutePath() + " to " + fileTo.getName());
            return false;
        }
        return true;
    }

    private abstract static class ChainLockStream<T extends Closeable> extends ChainedLock<String> implements ChainStream<T> {
        protected T currentStream;

        private ChainLockStream(Lock<String> lock) {
            super(lock);
        }

        @Override
        protected void initOthers(String value) {
            try {
                currentStream = createStream(value);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        protected abstract T createStream(String fileName) throws Exception;

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
                    closed = true;
                } catch (IOException e) {
                    log.warn(e.toString());
                    throw new RuntimeException(e);
                }
            }
            super.close();
        }
    }

    private static class ChainLockInputStream extends ChainLockStream<InputStream> {
        private ChainLockInputStream(Lock<String> lock) {
            super(lock);
        }

        @Override
        protected InputStream createStream(String fileName) throws FileNotFoundException {
            if (!new File(fileName).exists()) {
                return null;
            }
            return new BufferedInputStream(new FileInputStream(fileName));
        }
    }

    private static class ChainLockOutputStream extends ChainLockStream<OutputStream> {
        private ChainLockOutputStream(Lock<String> lock) {
            super(lock);
        }

        @Override
        protected OutputStream createStream(String fileName) throws Exception {
            return new BufferedOutputStream(new FileOutputStream(fileName), 10000);
        }
    }
}
