import org.junit.Test;
import server.model.FileHelper;
import server.model.ObjectConverter;
import server.model.impl.FileHelperImpl;
import server.model.impl.ObjectConverterImpl;
import server.model.pojo.Row;
import server.model.pojo.RowAddress;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class FileHelperTest {

    private final FileHelper fileHelper = new FileHelperImpl();
    private final ObjectConverter objectConverter = new ObjectConverterImpl();

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
            assertBytes(new byte[]{1, 5, 10, 2, 6, 11}, result);
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
            assertBytes(new byte[]{2, 6, 11}, result);
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
                assertBytes(objectConverter.toBytes(rowList.get(i)), fileHelper.read(rowAddressList.get(i)));
            }
        } finally {
            new File("test").delete();
        }
    }

    private void assertBytes(byte[] expected, byte[] actual) {
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i]);
        }
    }

    @Test
    public void collectTest() {
        try {
            final byte[] bytes1 = new byte[]{1, 3, 4, 8, 6};
            final byte[] bytes2 = new byte[]{2, 9, 11, 4, 7};
            final RowAddress rowAddress = new RowAddress("test", 1, 0, 5);
            fileHelper.write("test", bytes1, true);
            final byte[] bytes3 = new byte[5];
            fileHelper.collect(rowAddress, (inputStream, outputStream) -> {
                inputStream.read(bytes3);
                outputStream.write(bytes2);
            });
            assertBytes(bytes1, bytes3);
            assertBytes(bytes2, fileHelper.read(rowAddress));
        } finally {
            new File("test").delete();
        }
        try {
            final byte[] bytes1 = new byte[]{1, 3, 4, 8, 6};
            final byte[] bytes2 = new byte[]{2, 9, 11, 4, 7};
            final byte[] bytes3 = new byte[]{5, 8, 12, 94, 16};
            final RowAddress rowAddress1 = new RowAddress("test", 1, 0, 5);
            final RowAddress rowAddress2 = new RowAddress("test", 2, 5, 5);
            final RowAddress rowAddress3 = new RowAddress("test", 3, 10, 5);
            fileHelper.write("test", bytes1, true);
            fileHelper.write("test", bytes2, true);
            fileHelper.write("test", bytes3, true);
            assertBytes(bytes1, fileHelper.read(rowAddress1));
            assertBytes(bytes2, fileHelper.read(rowAddress2));
            assertBytes(bytes3, fileHelper.read(rowAddress3));
            final byte[] bytes6 = new byte[5];
            fileHelper.collect(rowAddress1, (inputStream, outputStream) -> {
                inputStream.read(bytes6);
            });
            assertBytes(bytes1, bytes6);
            assertBytes(new byte[5], fileHelper.read(rowAddress3));
            final byte[] bytes7 = new byte[5];
            fileHelper.collect(rowAddress1, (inputStream, outputStream) -> {
                inputStream.read(bytes7);
                outputStream.write(bytes7);
            });
            assertBytes(bytes2, bytes7);
            assertBytes(bytes2, fileHelper.read(rowAddress1));
            assertBytes(bytes3, fileHelper.read(rowAddress2));
            final byte[] bytes8 = new byte[5];
            fileHelper.collect(rowAddress1, (inputStream, outputStream) -> {
                inputStream.skip(5);
                outputStream.write(bytes8);
            });
            assertBytes(bytes8, fileHelper.read(rowAddress1));
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
            assertBytes(bytes1, bytes3);
            assertFalse(chainInputStream.isClosed());
            chainInputStream.init("test2");
            final byte[] bytes4 = new byte[5];
            chainInputStream.getStream().read(bytes4);
            assertBytes(bytes2, bytes4);
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
                assertBytes(bytes1, bytes3);
            }
            final byte[] bytes4 = new byte[5];
            try (InputStream inputStream = new FileInputStream("test2")) {
                inputStream.read(bytes4);
                assertBytes(bytes2, bytes4);
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
    public void collectListTest() {
        try {

            final boolean[] processed = new boolean[4];
            final boolean[] processedRunnableList = new boolean[4];
            final List<byte[]> byteList = new ArrayList<>();
            final List<RowAddress> rowAddresses = new ArrayList<>();
            final List<FileHelper.CollectBean> list = new ArrayList<>();
            {
                final String fileName = "temp1";
                final int size = 5;
                final byte[] bytesAdded = new byte[5];
                for (int i = 0; i < size; i++) {
                    bytesAdded[i] = (byte) i;
                }
                fileHelper.write(fileName, bytesAdded, true);
                final RowAddress rowAddress = new RowAddress(fileName, 0, 0, 5);
                final FileHelper.InputOutputConsumer inputOutputConsumer = (inputStream, outputStream) -> {
                    final byte[] oldBytes = new byte[rowAddress.getSize()];
                    inputStream.read(oldBytes);
                    assertBytes(bytesAdded, oldBytes);
                    processed[0] = true;
                    final byte[] bytes = new byte[rowAddress.getSize() + 2];
                    for (int i = 0; i < bytes.length; i++) {
                        bytes[i] = (byte) i;
                    }
                    byteList.add(bytes);
                    outputStream.write(bytes);
                    rowAddresses.add(new RowAddress(fileName, 0, 0, rowAddress.getSize() + 2));
                };
                list.add(new FileHelper.CollectBean(rowAddress, inputOutputConsumer, () -> processedRunnableList[0] = true));
            }
            {
                final String fileName = "temp1";
                final int size = 6;
                final byte[] bytesAdded = new byte[size];
                for (int i = 0; i < size; i++) {
                    bytesAdded[i] = (byte) i;
                }
                fileHelper.write(fileName, bytesAdded, true);
                final RowAddress rowAddress = new RowAddress(fileName, 2, 5, size);
                final FileHelper.InputOutputConsumer inputOutputConsumer = (inputStream, outputStream) -> {
                    final byte[] oldBytes = new byte[rowAddress.getSize()];
                    inputStream.read(oldBytes);
                    assertBytes(bytesAdded, oldBytes);
                    processed[1] = true;
                    final byte[] bytes = new byte[rowAddress.getSize() - 1];
                    for (int i = 0; i < bytes.length; i++) {
                        bytes[i] = (byte) i;
                    }
                    byteList.add(bytes);
                    outputStream.write(bytes);
                    rowAddresses.add(new RowAddress(fileName, 1, 7, rowAddress.getSize() - 1));
                };
                list.add(new FileHelper.CollectBean(rowAddress, inputOutputConsumer, () -> processedRunnableList[1] = true));
            }
            {
                final String fileName = "temp1";
                final int size = 7;
                final byte[] bytesAdded = new byte[size];
                for (int i = 0; i < size; i++) {
                    bytesAdded[i] = (byte) i;
                }
                fileHelper.write(fileName, bytesAdded, true);
                byteList.add(bytesAdded);
                rowAddresses.add(new RowAddress("temp1", 2, 12, size));
            }
            {
                final String fileName = "temp2";
                final int size = 7;
                final byte[] bytesAdded = new byte[size];
                for (int i = 0; i < size; i++) {
                    bytesAdded[i] = (byte) i;
                }
                fileHelper.write(fileName, bytesAdded, true);
                final RowAddress rowAddress = new RowAddress(fileName, 3, 0, size);
                final FileHelper.InputOutputConsumer inputOutputConsumer = (inputStream, outputStream) -> {
                    final byte[] oldBytes = new byte[rowAddress.getSize()];
                    inputStream.read(oldBytes);
                    assertBytes(bytesAdded, oldBytes);
                    processed[2] = true;
                    final byte[] bytes = new byte[rowAddress.getSize() + 3];
                    for (int i = 0; i < bytes.length; i++) {
                        bytes[i] = (byte) i;
                    }
                    byteList.add(bytes);
                    outputStream.write(bytes);
                    rowAddresses.add(new RowAddress(fileName, 3, 0, rowAddress.getSize() + 3));
                };
                list.add(new FileHelper.CollectBean(rowAddress, inputOutputConsumer, () -> processedRunnableList[2] = true));
            }
            {
                final String fileName = "temp2";
                final RowAddress rowAddress = new RowAddress(fileName, 0, 7, 9);
                final FileHelper.InputOutputConsumer inputOutputConsumer = (inputStream, outputStream) -> {
                    processed[3] = true;
                    final byte[] bytes = new byte[rowAddress.getSize()];
                    for (int i = 0; i < bytes.length; i++) {
                        bytes[i] = (byte) i;
                    }
                    byteList.add(bytes);
                    outputStream.write(bytes);
                    rowAddresses.add(new RowAddress(fileName, 3, 10, rowAddress.getSize()));
                };
                list.add(new FileHelper.CollectBean(rowAddress, inputOutputConsumer, () -> processedRunnableList[3] = true));
            }
            fileHelper.collect(list);
            for (boolean processedSingle : processed) {
                assertTrue(processedSingle);
            }
            for (boolean processedRunnable : processedRunnableList) {
                assertTrue(processedRunnable);
            }
            assertEquals(5, rowAddresses.size());
            int i = 0;
            for (RowAddress rowAddress : rowAddresses) {
                assertBytes(byteList.get(i), fileHelper.read(rowAddress));
                i++;
            }
        } finally {
            new File("temp1").delete();
            new File("temp2").delete();
        }
    }
}
