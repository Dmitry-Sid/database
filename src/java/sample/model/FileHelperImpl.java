package sample.model;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import sample.model.pojo.Pair;
import sample.model.pojo.RowAddress;

import java.io.*;
import java.util.function.Consumer;

public class FileHelperImpl implements FileHelper {
    private static final Logger log = LoggerFactory.getLogger(FileHelperImpl.class);

    @Override
    public void write(String fileName, byte[] bytes, boolean append) {
        LockService.doInFileLock(fileName, () -> {
            try (BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(fileName, append))) {
                output.write(bytes);
                output.flush();
            } catch (IOException e) {
                throw new RuntimeException();
            }
            return null;
        });
    }

    @Override
    public byte[] read(RowAddress rowAddress) {
        return LockService.doInFileLock(rowAddress.getFilePath(), () -> {
            try (FileInputStream fis = new FileInputStream(rowAddress.getFilePath())) {
                fis.skip(rowAddress.getPosition());
                final byte[] bytes = new byte[rowAddress.getSize()];
                fis.read(bytes);
                return bytes;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void collect(RowAddress rowAddress, InputOutputConsumer inputOutputConsumer) {
        actionWithRollBack(rowAddress.getFilePath(), (pair) -> {
            try (FileInputStream input = new FileInputStream(pair.getFirst());
                 BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(pair.getSecond()), 10000)) {
                long position = 0;
                int bit;
                while ((bit = input.read()) != -1) {
                    if (position == rowAddress.getPosition()) {
                        inputOutputConsumer.accept(input, output);
                    } else {
                        output.write(bit);
                    }
                    position++;
                }
                output.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void actionWithRollBack(String fileName, Consumer<Pair<File, File>> consumer) {
        final String tempFile = fileName + ".tmp";
        LockService.doInFileLock(fileName, () -> {
            LockService.doInFileLock(tempFile, () -> {
                final Pair<File, File> pair = new Pair<>(new File(fileName), new File(tempFile));
                try {
                    consumer.accept(pair);
                    if (delete(pair.getFirst())) {
                        rename(pair.getSecond(), pair.getFirst());
                    }
                } catch (Throwable throwable) {
                    delete(pair.getSecond());
                    log.error("error", throwable);
                    throw throwable;
                }
                return null;
            });
            return null;
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
        if (fileFrom == null || fileTo == null) {
            return false;
        }
        if (!fileFrom.renameTo(fileTo)) {
            log.warn("cannot rename file " + fileFrom.getAbsolutePath() + " to " + fileTo.getName());
            return false;
        }
        return true;
    }
}
