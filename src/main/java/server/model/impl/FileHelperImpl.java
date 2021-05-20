package server.model.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.model.FileHelper;
import server.model.lock.Lock;
import server.model.lock.LockService;
import server.model.lock.ReadWriteLock;
import server.model.pojo.Pair;
import server.model.pojo.RowAddress;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
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
        if (inputStream == null) {
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
        return new ChainLockInputStream();
    }

    @Override
    public ChainStream<OutputStream> getChainOutputStream() {
        return new ChainLockOutputStream();
    }

    @Override
    public void collect(CollectBean collectBean) {
        actionWithRollBack(collectBean.rowAddress.getFilePath(), (pair) -> {
            try (InputStream input = new FileInputStream(pair.getFirst());
                 OutputStream output = new BufferedOutputStream(new FileOutputStream(pair.getSecond()), 10000)) {
                int bit;
                if (collectBean.rowAddress.getPosition() == 0) {
                    collectBean.inputOutputConsumer.accept(input, output);
                    if (collectBean.runnable != null) {
                        collectBean.runnable.run();
                    }
                    while ((bit = input.read()) != -1) {
                        output.write(bit);
                    }
                } else {
                    long position = 0;
                    while ((bit = input.read()) != -1) {
                        if (position == collectBean.rowAddress.getPosition() - 1) {
                            collectBean.inputOutputConsumer.accept(input, output);
                            if (collectBean.runnable != null) {
                                collectBean.runnable.run();
                            }
                        } else {
                            output.write(bit);
                        }
                        position++;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void collect(List<CollectBean> list) {
        if (list == null || list.isEmpty()) {
            return;
        }
        String inputFileName = null;
        String tempFileName = null;
        long inputLastPosition = 0;
        int bit;
        final List<Runnable> runnableList = new ArrayList<>();
        try (ChainStream<InputStream> chainInputStream = getChainInputStream();
             ChainStream<OutputStream> chainOutputStream = getChainOutputStream()) {
            log.info("processing saved rows");
            final AtomicLong counter = new AtomicLong();
            for (CollectBean collectBean : list) {
                final RowAddress rowAddress = collectBean.rowAddress;
                if (inputFileName == null) {
                    inputFileName = rowAddress.getFilePath();
                    tempFileName = getTempFile(inputFileName);
                    chainInputStream.init(inputFileName);
                    chainOutputStream.init(tempFileName);
                } else if (!inputFileName.equals(rowAddress.getFilePath())) {
                    writeToEnd(chainInputStream, chainOutputStream);
                    inputLastPosition = 0;
                    chainInputStream.init(rowAddress.getFilePath());
                    chainOutputStream.init(getTempFile(rowAddress.getFilePath()));
                    saveTempFile(tempFileName, inputFileName, runnableList);
                    inputFileName = rowAddress.getFilePath();
                    tempFileName = getTempFile(inputFileName);
                }
                boolean found = false;
                if (chainInputStream.getStream() != null) {
                    chainInputStream.getStream().mark(1);
                    while ((bit = chainInputStream.getStream().read()) != -1) {
                        if (inputLastPosition == rowAddress.getPosition()) {
                            chainInputStream.getStream().reset();
                            collectBean.inputOutputConsumer.accept(chainInputStream.getStream(), chainOutputStream.getStream());
                            inputLastPosition = rowAddress.getPosition() + rowAddress.getSize();
                            found = true;
                            break;
                        } else {
                            chainOutputStream.getStream().write(bit);
                        }
                        chainInputStream.getStream().mark(1);
                        inputLastPosition++;
                    }
                }
                if (!found) {
                    if (chainInputStream.getStream() != null) {
                        chainInputStream.getStream().reset();
                    }
                    collectBean.inputOutputConsumer.accept(chainInputStream.getStream(), chainOutputStream.getStream());
                }
                runnableList.add(collectBean.runnable);
                if (counter.incrementAndGet() % 1000 == 0) {
                    log.info("processed " + counter.get() + " saved rows");
                }
            }
            writeToEnd(chainInputStream, chainOutputStream);
            log.info("processing saved rows done, count " + counter.get());
        } catch (IOException e) {
            delete(new File(tempFileName));
            throw new RuntimeException(e);
        }
        if (inputFileName != null) {
            saveTempFile(tempFileName, inputFileName, runnableList);
        }
    }

    private void saveTempFile(String tempFileName, String inputFileName, List<Runnable> runnableList) {
        LockService.doInLock(readWriteLock.writeLock(), inputFileName, () -> {
            for (Runnable runnable : runnableList) {
                if (runnable != null) {
                    runnable.run();
                }
            }
            runnableList.clear();
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

    private void actionWithRollBack(String fileName, Consumer<Pair<File, File>> consumer) {
        LockService.doInLock(readWriteLock.writeLock(), fileName, () -> {
            final String tempFile = getTempFile(fileName);
            final Pair<File, File> pair = new Pair<>(new File(fileName), new File(tempFile));
            try {
                consumer.accept(pair);
                deleteAndRename(pair.getSecond(), pair.getFirst());
            } catch (Throwable throwable) {
                delete(pair.getSecond());
                log.error("error", throwable);
                throw throwable;
            }
        });
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

    private abstract static class ChainLockStream<T extends Closeable> implements ChainStream<T> {
        T currentStream;
        private String currentFileName;
        private boolean closed = true;

        public void init(String fileName) {
            close();
            try {
                getLock().lock(fileName);
                currentFileName = fileName;
                currentStream = createStream(fileName);
                closed = false;
            } catch (Throwable e) {
                close();
                throw new RuntimeException(e);
            }
        }

        protected abstract T createStream(String fileName) throws Exception;

        protected abstract Lock<String> getLock();

        public String getFileName() {
            return currentFileName;
        }

        public T getStream() {
            return currentStream;
        }

        @Override
        public void close() {
            if (currentStream != null) {
                try {
                    currentStream.close();
                    closed = true;
                } catch (IOException e) {
                    log.warn(e.toString());
                }
            }
            if (currentFileName != null) {
                getLock().unlock(currentFileName);
            }
        }

        public boolean isClosed() {
            return closed;
        }
    }

    public class ChainLockInputStream extends ChainLockStream<InputStream> {
        @Override
        protected InputStream createStream(String fileName) throws FileNotFoundException {
            if (!new File(fileName).exists()) {
                return null;
            }
            return new BufferedInputStream(new FileInputStream(fileName));
        }

        @Override
        protected Lock<String> getLock() {
            return readWriteLock.readLock();
        }
    }

    public class ChainLockOutputStream extends ChainLockStream<OutputStream> {
        @Override
        protected OutputStream createStream(String fileName) throws Exception {
            return new BufferedOutputStream(new FileOutputStream(fileName), 10000);
        }

        @Override
        protected Lock<String> getLock() {
            return readWriteLock.writeLock();
        }
    }
}
