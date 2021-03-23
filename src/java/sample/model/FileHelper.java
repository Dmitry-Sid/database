package sample.model;

import sample.model.pojo.Pair;
import sample.model.pojo.RowAddress;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public interface FileHelper {

    public void write(String fileName, byte[] bytes, boolean append);

    public byte[] read(RowAddress rowAddress);

    public void skip(InputStream inputStream, long size);

    public ChainInputStream getChainInputStream();

    public ChainOutputStream getChainOutputStream();

    public void collect(RowAddress rowAddress, InputOutputConsumer inputOutputConsumer);

    public void collect(List<CollectBean> list);

    public interface InputOutputConsumer {
        void accept(InputStream inputStream, OutputStream outputStream) throws IOException;
    }

    public interface ChainInputStream extends Closeable {
        public void read(String fileName);

        public String getFileName();

        public InputStream getInputStream();

        public boolean isClosed();
    }

    public interface ChainOutputStream extends Closeable {
        public void init(String fileName);

        public String getFileName();

        public OutputStream getOutputStream();

        public boolean isClosed();
    }

    public static class CollectBean {
        public final RowAddress rowAddress;
        public final InputOutputConsumer inputOutputConsumer;
        public final Runnable runnable;

        public CollectBean(RowAddress rowAddress, InputOutputConsumer inputOutputConsumer, Runnable runnable) {
            this.rowAddress = rowAddress;
            this.inputOutputConsumer = inputOutputConsumer;
            this.runnable = runnable;
        }
    }
}
