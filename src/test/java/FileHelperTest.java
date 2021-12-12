import org.apache.commons.io.FileUtils;
import org.junit.Test;
import server.model.*;
import server.model.impl.DataCompressorImpl;
import server.model.impl.FileHelperImpl;
import server.model.impl.ObjectConverterImpl;
import server.model.pojo.Pair;
import server.model.pojo.Row;
import server.model.pojo.RowAddress;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.Assert.*;

public class FileHelperTest {

    private final FileHelper fileHelper = new FileHelperImpl();
    private final ObjectConverter objectConverter = new ObjectConverterImpl(new DataCompressorImpl());

    @Test
    public void writeTest() throws IOException {
        try {
            final byte[] firstBytes = new byte[]{1, 5, 10};
            try (FileOutputStream outputStream = new FileOutputStream("test")) {
                outputStream.write(firstBytes);
            }
            final byte[] secondBytes = new byte[]{2, 6, 11};
            fileHelper.write("test", secondBytes, true);
            final byte[] result = new byte[6];
            int index = 0;
            try (FileInputStream inputStream = new FileInputStream("test")) {
                byte bit;
                while ((bit = (byte) inputStream.read()) > -1) {
                    result[index] = bit;
                    index++;
                }
            }
            assertEquals(6, index);
            TestUtils.assertBytes(new byte[]{1, 5, 10, 2, 6, 11}, result);
        } finally {
            new File("test").delete();
        }
        try {
            final byte[] firstBytes = new byte[]{1, 5, 10};
            try (FileOutputStream outputStream = new FileOutputStream("test")) {
                outputStream.write(firstBytes);
            }
            final byte[] secondBytes = new byte[]{2, 6, 11};
            fileHelper.write("test", secondBytes, false);
            final byte[] result = new byte[3];
            int index = 0;
            try (FileInputStream inputStream = new FileInputStream("test")) {
                byte bit;
                while ((bit = (byte) inputStream.read()) > -1) {
                    result[index] = bit;
                    index++;
                }
            }
            assertEquals(3, index);
            TestUtils.assertBytes(new byte[]{2, 6, 11}, result);
        } finally {
            new File("test").delete();
        }
    }

    @Test
    public void readAndWriteTest() {
        try {
            final List<Row> rowList = new ArrayList<>();
            final List<RowAddress> rowAddressList = new ArrayList<>();
            long lastPosition = 0;
            for (int i = 0; i < 3; i++) {
                rowList.add(TestUtils.generateRow(i, i));
                final byte[] bytes = objectConverter.toBytes(rowList.get(i));
                rowAddressList.add(new RowAddress("test", i, lastPosition, bytes.length));
                lastPosition += bytes.length;
                fileHelper.write("test", bytes, true);
            }
            for (int i = 0; i < 3; i++) {
                TestUtils.assertBytes(objectConverter.toBytes(rowList.get(i)), fileHelper.read(rowAddressList.get(i)));
            }
        } finally {
            new File("test").delete();
        }
    }

    @Test
    public void chainInputStreamTest() throws IOException {
        try {
            final FileHelper.ChainStream<InputStream> chainInputStream = fileHelper.getChainInputStream();
            final byte[] bytes1 = new byte[]{1, 3, 4, 8, 6};
            final byte[] bytes2 = new byte[]{2, 9, 11, 4, 7};
            fileHelper.write("test1", bytes1, true);
            fileHelper.write("test2", bytes2, true);
            assertTrue(chainInputStream.isClosed());
            chainInputStream.init("test1");
            assertFalse(chainInputStream.isClosed());
            final byte[] bytes3 = new byte[5];
            chainInputStream.getStream().read(bytes3);
            TestUtils.assertBytes(bytes1, bytes3);
            assertFalse(chainInputStream.isClosed());
            chainInputStream.init("test2");
            final byte[] bytes4 = new byte[5];
            chainInputStream.getStream().read(bytes4);
            TestUtils.assertBytes(bytes2, bytes4);
            chainInputStream.close();
            assertTrue(chainInputStream.isClosed());
        } finally {
            new File("test1").delete();
            new File("test2").delete();
        }
    }

    @Test
    public void chainOutputStream() throws IOException {
        try {
            final FileHelper.ChainStream<OutputStream> chainOutputStream = fileHelper.getChainOutputStream();
            final byte[] bytes1 = new byte[]{1, 3, 4, 8, 6};
            final byte[] bytes2 = new byte[]{2, 9, 11, 4, 7};
            assertTrue(chainOutputStream.isClosed());
            chainOutputStream.init("test1");
            assertFalse(chainOutputStream.isClosed());
            chainOutputStream.getStream().write(bytes1);
            chainOutputStream.init("test2");
            chainOutputStream.getStream().write(bytes2);
            assertFalse(chainOutputStream.isClosed());
            chainOutputStream.close();
            assertTrue(chainOutputStream.isClosed());
            final byte[] bytes3 = new byte[5];
            try (InputStream inputStream = new FileInputStream("test1")) {
                inputStream.read(bytes3);
                TestUtils.assertBytes(bytes1, bytes3);
            }
            final byte[] bytes4 = new byte[5];
            try (InputStream inputStream = new FileInputStream("test2")) {
                inputStream.read(bytes4);
                TestUtils.assertBytes(bytes2, bytes4);
            }
        } finally {
            new File("test1").delete();
            new File("test2").delete();
        }
    }

    @Test
    public void skipTest() throws IOException {
        int maxSize = 15000;
        final int size = 10000;
        final byte[] bytes = new byte[maxSize];
        for (int i = 0; i < maxSize; i++) {
            bytes[i] = (byte) i;
        }
        try (InputStream inputStream = new BufferedInputStream(new ByteArrayInputStream(bytes), 5000)) {
            fileHelper.skip(inputStream, size);
            final byte[] bytesFrom = new byte[maxSize - size];
            assertEquals(bytesFrom.length, inputStream.read(bytesFrom));
            for (int i = 0; i < bytesFrom.length; i++) {
                assertEquals((byte) (i + size), bytesFrom[i]);
            }
        }
    }

    @Test
    public void collectTest() throws IOException {
        final String directory = "row";
        Utils.createDirectoryTree(new File(directory));
        try {
            final Pair<List<Row>, List<RowAddress>> pair = makePair(directory, 0);
            for (int i = 0; i < 6; i++) {
                final byte[] bytes = objectConverter.toBytes(pair.getFirst().get(i));
                fileHelper.write(pair.getSecond().get(i).getFilePath(), bytes, true);
            }
            for (int i = 0; i < 6; i++) {
                TestUtils.assertBytes(objectConverter.toBytes(pair.getFirst().get(i)), fileHelper.read(pair.getSecond().get(i)));
            }
            final StoppableBatchStream<RowAddress> stream = new BaseStoppableBatchStream<RowAddress>() {
                @Override
                public void forEach(Consumer<RowAddress> consumer) {
                    pair.getSecond().forEach(rowAddress -> {
                        if (rowAddress.getId() == 4) {
                            onBatchEnd.forEach(Runnable::run);
                        }
                        consumer.accept(rowAddress);
                    });
                    onBatchEnd.forEach(Runnable::run);
                }
            };
            final Pair<List<Row>, List<RowAddress>> changedPair = makePair(directory, 1);
            final AtomicInteger counter = new AtomicInteger(0);
            fileHelper.collect(stream, collectBean -> {
                assertEquals(pair.getSecond().get(counter.get()), collectBean.rowAddress);
                fileHelper.skip(collectBean.inputStream, collectBean.rowAddress.getSize());
                try {
                    collectBean.outputStream.write(objectConverter.toBytes(changedPair.getFirst().get(counter.getAndIncrement())));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            for (int i = 0; i < 6; i++) {
                TestUtils.assertBytes(objectConverter.toBytes(changedPair.getFirst().get(i)), fileHelper.read(changedPair.getSecond().get(i)));
            }
        } finally {
            assertEquals(0, Objects.requireNonNull(new File(directory).listFiles((dir, name) -> name.endsWith(".tmp"))).length);
            FileUtils.deleteDirectory(new File(directory));
        }
    }

    private Pair<List<Row>, List<RowAddress>> makePair(String directory, int additionalParameter) {
        final List<Row> rowList = new ArrayList<>();
        final List<RowAddress> rowAddressList = new ArrayList<>();
        long lastPosition = 0;
        for (int i = 0; i < 6; i++) {
            rowList.add(TestUtils.generateRow(i, i + additionalParameter));
            final byte[] bytes = objectConverter.toBytes(rowList.get(i));
            final int fileNumber;
            if (i == 0 || i == 1) {
                fileNumber = 1;
            } else if (i == 2 || i == 3) {
                fileNumber = 2;
                if (i == 2) {
                    lastPosition = 0;
                }
            } else {
                fileNumber = 3;
                if (i == 4) {
                    lastPosition = 0;
                }
            }
            final RowAddress rowAddress = new RowAddress(directory + "/row" + fileNumber, i, lastPosition, bytes.length);
            rowAddressList.add(rowAddress);
            lastPosition += bytes.length;
        }
        return new Pair<>(rowList, rowAddressList);
    }
}