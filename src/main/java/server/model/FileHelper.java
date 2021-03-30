package server.model;

import server.model.pojo.RowAddress;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public interface FileHelper {

    void write(String fileName, byte[] bytes, boolean append);

    byte[] read(RowAddress rowAddress);

    void skip(InputStream inputStream, long size);

    ChainInputStream getChainInputStream();

    ChainOutputStream getChainOutputStream();

    void collect(RowAddress rowAddress, InputOutputConsumer inputOutputConsumer);

    void collect(List<CollectBean> list);

    interface InputOutputConsumer {
        void accept(InputStream inputStream, OutputStream outputStream) throws IOException;
    }

    interface ChainInputStream extends Closeable {
        void read(String fileName);

        String getFileName();

        InputStream getInputStream();

        boolean isClosed();
    }

    interface ChainOutputStream extends Closeable {
        void init(String fileName);

        String getFileName();

        OutputStream getOutputStream();

        boolean isClosed();
    }

    class CollectBean {
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
