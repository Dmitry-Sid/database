package server.model;

import server.model.pojo.RowAddress;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.function.Consumer;

public interface FileHelper {

    void write(String fileName, byte[] bytes, boolean append);

    byte[] read(RowAddress rowAddress);

    void skip(InputStream inputStream, long size);

    ChainStream<InputStream> getChainInputStream();

    ChainStream<OutputStream> getChainOutputStream();

    void collect(CollectBean collectBean);

    void collect(List<CollectBean> list);

    void collect(StoppableStream<RowAddress> stoppableStream, Consumer<CollectBean2> consumer);

    interface InputOutputConsumer {
        void accept(InputStream inputStream, OutputStream outputStream) throws IOException;
    }

    interface ChainStream<T extends Closeable> extends Closeable {
        void init(String fileName);

        String getFileName();

        T getStream();

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

    class CollectBean2 {
        public final RowAddress rowAddress;
        public final InputStream inputStream;
        public final OutputStream outputStream;
        public final List<Runnable> runnableList;

        public CollectBean2(RowAddress rowAddress, InputStream inputStream, OutputStream outputStream, List<Runnable> runnableList) {
            this.rowAddress = rowAddress;
            this.inputStream = inputStream;
            this.outputStream = outputStream;
            this.runnableList = runnableList;
        }
    }
}
