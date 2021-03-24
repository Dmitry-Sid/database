package server.model;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import server.model.lock.LockService;
import server.model.lock.ReadWriteLock;
import server.model.pojo.Pair;
import server.model.pojo.RowAddress;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class FileHelperImpl implements FileHelper {
    private static final Logger log = LoggerFactory.getLogger(FileHelperImpl.class);
    private final ReadWriteLock<String> readWriteLock = LockService.createReadWriteLock(String.class);

    @Override
    public void write(String fileName, byte[] bytes, boolean append) {
        LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Write, fileName, () -> {
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
        return LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Read, rowAddress.getFilePath(), () -> {
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
    public ChainInputStream getChainInputStream() {
        return new ChainLockInputStream();
    }

    @Override
    public ChainOutputStream getChainOutputStream() {
        return new ChainLockOutputStream();
    }

    @Override
    public void collect(RowAddress rowAddress, InputOutputConsumer inputOutputConsumer) {
        actionWithRollBack(rowAddress.getFilePath(), (pair) -> {
            try (InputStream input = new FileInputStream(pair.getFirst());
                 OutputStream output = new BufferedOutputStream(new FileOutputStream(pair.getSecond()), 10000)) {
                int bit;
                if (rowAddress.getPosition() == 0) {
                    inputOutputConsumer.accept(input, output);
                    while ((bit = input.read()) != -1) {
                        output.write(bit);
                    }
                } else {
                    long position = 0;
                    while ((bit = input.read()) != -1) {
                        if (position == rowAddress.getPosition() - 1) {
                            inputOutputConsumer.accept(input, output);
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
        String inputFileName = null;
        String tempFileName = null;
        long inputLastPosition = 0;
        int bit;
        final List<Runnable> runnableList = new ArrayList<>();
        try (ChainInputStream chainInputStream = getChainInputStream();
             ChainOutputStream chainOutputStream = getChainOutputStream()) {
            for (CollectBean collectBean : list) {
                final RowAddress rowAddress = collectBean.rowAddress;
                if (inputFileName == null) {
                    inputFileName = rowAddress.getFilePath();
                    tempFileName = getTempFile(inputFileName);
                    chainInputStream.read(inputFileName);
                    chainOutputStream.init(tempFileName);
                } else if (!inputFileName.equals(rowAddress.getFilePath())) {
                    writeToEnd(chainInputStream, chainOutputStream);
                    inputLastPosition = 0;
                    chainInputStream.read(rowAddress.getFilePath());
                    chainOutputStream.init(getTempFile(rowAddress.getFilePath()));
                    saveTempFile(tempFileName, inputFileName, runnableList);
                    inputFileName = rowAddress.getFilePath();
                    tempFileName = getTempFile(inputFileName);
                }
                boolean found = false;
                if (chainInputStream.getInputStream() != null) {
                    chainInputStream.getInputStream().mark(1);
                    while ((bit = chainInputStream.getInputStream().read()) != -1) {
                        if (inputLastPosition == rowAddress.getPosition()) {
                            chainInputStream.getInputStream().reset();
                            collectBean.inputOutputConsumer.accept(chainInputStream.getInputStream(), chainOutputStream.getOutputStream());
                            inputLastPosition = rowAddress.getPosition() + rowAddress.getSize();
                            found = true;
                            break;
                        } else {
                            chainOutputStream.getOutputStream().write(bit);
                        }
                        chainInputStream.getInputStream().mark(1);
                        inputLastPosition++;
                    }
                }
                if (!found) {
                    if (chainInputStream.getInputStream() != null) {
                        chainInputStream.getInputStream().reset();
                    }
                    collectBean.inputOutputConsumer.accept(chainInputStream.getInputStream(), chainOutputStream.getOutputStream());
                }
                runnableList.add(collectBean.runnable);
            }
            writeToEnd(chainInputStream, chainOutputStream);
        } catch (IOException e) {
            delete(new File(tempFileName));
            throw new RuntimeException(e);
        }
        if (inputFileName != null) {
            saveTempFile(tempFileName, inputFileName, runnableList);
        }
    }

    private void saveTempFile(String tempFileName, String inputFileName, List<Runnable> runnableList) {
        LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Write, inputFileName, () -> {
            for (Runnable runnable : runnableList) {
                if (runnable != null) {
                    runnable.run();
                }
            }
            runnableList.clear();
            deleteAndRename(new File(tempFileName), new File(inputFileName));
        });
    }

    private void writeToEnd(ChainInputStream chainInputStream, ChainOutputStream chainOutputStream) throws IOException {
        if (chainInputStream.getInputStream() != null) {
            int bit;
            while ((bit = chainInputStream.getInputStream().read()) != -1) {
                chainOutputStream.getOutputStream().write(bit);
            }
        }
    }

    private String getTempFile(String fileName) {
        return fileName + ".tmp";
    }

    private void actionWithRollBack(String fileName, Consumer<Pair<File, File>> consumer) {
        final String tempFile = getTempFile(fileName);
        LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Read, fileName, () -> {
            LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Write, tempFile, () -> {
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
        });
    }

    private boolean deleteAndRename(File fileFrom, File fileTo) {
        return LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Write, fileTo.getName(), () -> {
            if (!fileTo.exists() || delete(fileTo)) {
                return rename(fileFrom, fileTo);
            }
            return false;
        });
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

    public class ChainLockInputStream extends ChainLockStream<InputStream> implements ChainInputStream {

        public void read(String fileName) {
            init(fileName);
        }

        @Override
        public InputStream getInputStream() {
            return currentStream;
        }

        @Override
        protected InputStream createStream(String fileName) throws Exception {
            if (!new File(fileName).exists()) {
                return null;
            }
            return new BufferedInputStream(new FileInputStream(fileName));
        }

        @Override
        protected void lock(String fileName) {
            readWriteLock.readLock(fileName);
        }

        @Override
        protected void unlock(String fileName) {
            readWriteLock.readUnlock(fileName);
        }
    }

    public class ChainLockOutputStream extends ChainLockStream<OutputStream> implements ChainOutputStream {

        @Override
        public OutputStream getOutputStream() {
            return currentStream;
        }

        @Override
        protected OutputStream createStream(String fileName) throws Exception {
            return new BufferedOutputStream(new FileOutputStream(fileName), 10000);
        }

        @Override
        protected void lock(String fileName) {
            readWriteLock.readLock(fileName);
        }

        @Override
        protected void unlock(String fileName) {
            readWriteLock.readUnlock(fileName);
        }
    }

    private abstract static class ChainLockStream<T extends Closeable> {
        private String currentFileName;
        T currentStream;
        private boolean closed = true;

        public void init(String fileName) {
            close();
            lock(fileName);
            try {
                currentFileName = fileName;
                currentStream = createStream(fileName);
                closed = false;
            } catch (Throwable e) {
                close();
                throw new RuntimeException(e);
            }
        }

        protected abstract T createStream(String fileName) throws Exception;

        protected abstract void lock(String fileName);

        protected abstract void unlock(String fileName);

        public String getFileName() {
            return currentFileName;
        }

        public T getStream() {
            return currentStream;
        }

        public void close() {
            if (currentFileName != null) {
                unlock(currentFileName);
            }
            if (currentStream != null) {
                try {
                    currentStream.close();
                    closed = true;
                } catch (IOException e) {
                    log.warn(e.toString());
                }
            }
        }

        public boolean isClosed() {
            return closed;
        }
    }
}
