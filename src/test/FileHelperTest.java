import org.junit.Test;
import sample.model.FileHelper;
import sample.model.FileHelperImpl;
import sample.model.ObjectConverter;
import sample.model.ObjectConverterImpl;
import sample.model.pojo.Row;
import sample.model.pojo.RowAddress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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
            final FileHelper.ChainInputStream chainInputStream = fileHelper.getChainInputStream();
            final byte[] bytes1 = new byte[]{1, 3, 4, 8, 6};
            final byte[] bytes2 = new byte[]{2, 9, 11, 4, 7};
            fileHelper.write("test1", bytes1, true);
            fileHelper.write("test2", bytes2, true);
            assertTrue(chainInputStream.isClosed());
            chainInputStream.read("test1");
            assertFalse(chainInputStream.isClosed());
            final byte[] bytes3 = new byte[5];
            chainInputStream.getInputStream().read(bytes3);
            assertBytes(bytes1, bytes3);
            assertFalse(chainInputStream.isClosed());
            chainInputStream.read("test2");
            final byte[] bytes4 = new byte[5];
            chainInputStream.getInputStream().read(bytes4);
            assertBytes(bytes2, bytes4);
            chainInputStream.close();
            assertTrue(chainInputStream.isClosed());
        } finally {
            new File("test1").delete();
            new File("test2").delete();
        }
    }

}
