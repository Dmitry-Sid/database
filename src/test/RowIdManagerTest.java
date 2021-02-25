import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sample.model.RowIdManager;
import sample.model.pojo.RowAddress;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class RowIdManagerTest {

    private RowIdManager rowIdManager;
    private static final String fileName = "rowIdVariables.test";
    private static final String filesIdPath = "rowId";
    private static final String filesRowPath = "row";
    private static final int maxIdSize = 500;
    private static final int compressSize = 2;
    private static final int rowAddressSize = 5;

    @Before
    public void before() {
        createFiles(0);
        rowIdManager = prepareRowIdManager();
    }

    @After
    public void after() {
        new File(fileName).delete();
        new File(filesIdPath + 1).delete();
        new File(filesRowPath + 1).delete();
    }

    @Test
    public void processTest() {
        final int lastId = 750;
        createFiles(lastId);
        rowIdManager = prepareRowIdManager();
        try {
            assertFalse(rowIdManager.process(0, rowAddress -> {
            }));
            assertFalse(rowIdManager.process(1200, rowAddress -> {
            }));
            assertTrue(rowIdManager.process(1, rowAddress -> {
                assertEquals(filesRowPath + 1, rowAddress.getFilePath());
                assertEquals(1, rowAddress.getId());
                assertEquals(0, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + 1, 2, 5, rowAddressSize);
                assertEquals(rowAddressNext, rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(250, rowAddress -> {
                assertEquals(filesRowPath + 1, rowAddress.getFilePath());
                assertEquals(250, rowAddress.getId());
                assertEquals(1245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                assertNull(rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(499, rowAddress -> {
                assertEquals(filesRowPath + 2, rowAddress.getFilePath());
                assertEquals(499, rowAddress.getId());
                assertEquals(1240, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + 2, 500, 1245, rowAddressSize);
                assertEquals(rowAddressNext, rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(500, rowAddress -> {
                assertEquals(filesRowPath + 2, rowAddress.getFilePath());
                assertEquals(500, rowAddress.getId());
                assertEquals(1245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                assertNull(rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(501, rowAddress -> {
                assertEquals(filesRowPath + 3, rowAddress.getFilePath());
                assertEquals(501, rowAddress.getId());
                assertEquals(0, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + 3, 502, 5, rowAddressSize);
                assertEquals(rowAddressNext, rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(749, rowAddress -> {
                assertEquals(filesRowPath + 3, rowAddress.getFilePath());
                assertEquals(749, rowAddress.getId());
                assertEquals(1240, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + 3, 750, 1245, rowAddressSize);
                assertEquals(rowAddressNext, rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(750, rowAddress -> {
                assertEquals(filesRowPath + 3, rowAddress.getFilePath());
                assertEquals(750, rowAddress.getId());
                assertEquals(1245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                assertNull(rowAddress.getNext());
            }));
            assertFalse(rowIdManager.process(751, rowAddress -> {
            }));
        } finally {
            for (Integer value : TestUtils.prepareBoundsBatch(lastId, maxIdSize)) {
                new File(filesIdPath + value).delete();
            }
        }
    }

    @Test
    public void transformTest() {
        final int lastId = 750;
        createFiles(lastId);
        rowIdManager = prepareRowIdManager();
        try {
            rowIdManager.transform(501, 14);
            rowIdManager.transform(250, 7);
            assertTrue(rowIdManager.process(501, rowAddress -> {
                assertEquals(filesRowPath + 3, rowAddress.getFilePath());
                assertEquals(501, rowAddress.getId());
                assertEquals(0, rowAddress.getPosition());
                assertEquals(14, rowAddress.getSize());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + 3, 502, 14, rowAddressSize);
                assertEquals(rowAddressNext, rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(650, rowAddress -> {
                assertEquals(filesRowPath + 3, rowAddress.getFilePath());
                assertEquals(650, rowAddress.getId());
                assertEquals(754, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + 3, 651, 759, rowAddressSize);
                assertEquals(rowAddressNext, rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(750, rowAddress -> {
                assertEquals(filesRowPath + 3, rowAddress.getFilePath());
                assertEquals(750, rowAddress.getId());
                assertEquals(1254, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                assertNull(rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(1, rowAddress -> {
                assertEquals(filesRowPath + 1, rowAddress.getFilePath());
                assertEquals(1, rowAddress.getId());
                assertEquals(0, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + 1, 2, 5, rowAddressSize);
                assertEquals(rowAddressNext, rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(250, rowAddress -> {
                assertEquals(filesRowPath + 1, rowAddress.getFilePath());
                assertEquals(250, rowAddress.getId());
                assertEquals(1245, rowAddress.getPosition());
                assertEquals(7, rowAddress.getSize());
                assertNull(rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(499, rowAddress -> {
                assertEquals(filesRowPath + 2, rowAddress.getFilePath());
                assertEquals(499, rowAddress.getId());
                assertEquals(1240, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + 2, 500, 1245, rowAddressSize);
                assertEquals(rowAddressNext, rowAddress.getNext());
            }));
            rowIdManager.transform(100, 15);
            assertTrue(rowIdManager.process(99, rowAddress -> {
                assertEquals(filesRowPath + 1, rowAddress.getFilePath());
                assertEquals(99, rowAddress.getId());
                assertEquals(490, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + 1, 100, 495, 15);
                assertEquals(rowAddressNext, rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(100, rowAddress -> {
                assertEquals(filesRowPath + 1, rowAddress.getFilePath());
                assertEquals(100, rowAddress.getId());
                assertEquals(495, rowAddress.getPosition());
                assertEquals(15, rowAddress.getSize());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + 1, 101, 510, rowAddressSize);
                assertEquals(rowAddressNext, rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(250, rowAddress -> {
                assertEquals(filesRowPath + 1, rowAddress.getFilePath());
                assertEquals(250, rowAddress.getId());
                assertEquals(1255, rowAddress.getPosition());
                assertEquals(7, rowAddress.getSize());
                assertNull(rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(251, rowAddress -> {
                assertEquals(filesRowPath + 2, rowAddress.getFilePath());
                assertEquals(251, rowAddress.getId());
                assertEquals(0, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                assertNull(rowAddress.getPrevious());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + 2, 252, 5, rowAddressSize);
                assertEquals(rowAddressNext, rowAddress.getNext());
            }));
        } finally {
            for (Integer value : TestUtils.prepareBoundsBatch(lastId, maxIdSize)) {
                new File(filesIdPath + value).delete();
            }
        }
    }

    @Test
    public void addTest() {
        final int lastId = 750;
        createFiles(lastId);
        rowIdManager = prepareRowIdManager();
        try {
            rowIdManager.add(rowAddress -> {
                rowAddress.setSize(10);
                return true;
            });
            assertTrue(rowIdManager.process(250, rowAddress -> {
                assertEquals(filesRowPath + 1, rowAddress.getFilePath());
                assertEquals(250, rowAddress.getId());
                assertEquals(1245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                assertNull(rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(750, rowAddress -> {
                assertEquals(filesRowPath + 3, rowAddress.getFilePath());
                assertEquals(750, rowAddress.getId());
                assertEquals(1245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                assertNull(rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(751, rowAddress -> {
                assertEquals(filesRowPath + 4, rowAddress.getFilePath());
                assertEquals(751, rowAddress.getId());
                assertEquals(0, rowAddress.getPosition());
                assertEquals(10, rowAddress.getSize());
                assertNull(rowAddress.getPrevious());
                assertNull(rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(250, rowAddress -> {
                assertEquals(filesRowPath + 1, rowAddress.getFilePath());
                assertEquals(250, rowAddress.getId());
                assertEquals(1245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                assertNull(rowAddress.getNext());
            }));
            rowIdManager.add(rowAddress -> {
                rowAddress.setSize(67);
                return true;
            });
            assertTrue(rowIdManager.process(250, rowAddress -> {
                assertEquals(filesRowPath + 1, rowAddress.getFilePath());
                assertEquals(250, rowAddress.getId());
                assertEquals(1245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                assertNull(rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(750, rowAddress -> {
                assertEquals(filesRowPath + 3, rowAddress.getFilePath());
                assertEquals(750, rowAddress.getId());
                assertEquals(1245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                assertNull(rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(751, rowAddress -> {
                assertEquals(filesRowPath + 4, rowAddress.getFilePath());
                assertEquals(751, rowAddress.getId());
                assertEquals(0, rowAddress.getPosition());
                assertEquals(10, rowAddress.getSize());
                assertNull(rowAddress.getPrevious());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + 4, 752, 10, 67);
                assertEquals(rowAddressNext, rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(752, rowAddress -> {
                assertEquals(filesRowPath + 4, rowAddress.getFilePath());
                assertEquals(752, rowAddress.getId());
                assertEquals(10, rowAddress.getPosition());
                assertEquals(67, rowAddress.getSize());
                final RowAddress rowAddressPrevious = new RowAddress(filesRowPath + 4, 751, 0, 10);
                assertEquals(rowAddressPrevious, rowAddress.getPrevious());
                assertNull(rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(250, rowAddress -> {
                assertEquals(filesRowPath + 1, rowAddress.getFilePath());
                assertEquals(250, rowAddress.getId());
                assertEquals(1245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                assertNull(rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(752, rowAddress -> {
                assertEquals(filesRowPath + 4, rowAddress.getFilePath());
                assertEquals(752, rowAddress.getId());
                assertEquals(10, rowAddress.getPosition());
                assertEquals(67, rowAddress.getSize());
                final RowAddress rowAddressPrevious = new RowAddress(filesRowPath + 4, 751, 0, 10);
                assertEquals(rowAddressPrevious, rowAddress.getPrevious());
                assertNull(rowAddress.getNext());
            }));
        } finally {
            for (Integer value : TestUtils.prepareBoundsBatch(752, maxIdSize)) {
                new File(filesIdPath + value).delete();
            }
        }
    }

    @Test
    public void deleteTest() {
        final int lastId = 750;
        createFiles(lastId);
        rowIdManager = prepareRowIdManager();
        try {
            assertTrue(rowIdManager.process(300, rowAddress -> {
                assertEquals(filesRowPath + 2, rowAddress.getFilePath());
                assertEquals(300, rowAddress.getId());
                assertEquals(245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressPrevious = new RowAddress(filesRowPath + 2, 299, 240, rowAddressSize);
                assertEquals(rowAddressPrevious, rowAddress.getPrevious());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + 2, 301, 250, rowAddressSize);
                assertEquals(rowAddressNext, rowAddress.getNext());
            }));
            rowIdManager.delete(300);
            assertFalse(rowIdManager.process(300, rowAddress -> {

            }));
            assertTrue(rowIdManager.process(750, rowAddress -> {
                assertEquals(filesRowPath + 3, rowAddress.getFilePath());
                assertEquals(750, rowAddress.getId());
                assertEquals(1245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                assertNull(rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(299, rowAddress -> {
                assertEquals(filesRowPath + 2, rowAddress.getFilePath());
                assertEquals(299, rowAddress.getId());
                assertEquals(240, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressPrevious = new RowAddress(filesRowPath + 2, 298, 235, rowAddressSize);
                assertEquals(rowAddressPrevious, rowAddress.getPrevious());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + 2, 301, 245, rowAddressSize);
                assertEquals(rowAddressNext, rowAddress.getNext());
            }));
            assertTrue(rowIdManager.process(301, rowAddress -> {
                assertEquals(filesRowPath + 2, rowAddress.getFilePath());
                assertEquals(301, rowAddress.getId());
                assertEquals(245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressPrevious = new RowAddress(filesRowPath + 2, 299, 240, rowAddressSize);
                assertEquals(rowAddressPrevious, rowAddress.getPrevious());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + 2, 302, 250, rowAddressSize);
                assertEquals(rowAddressNext, rowAddress.getNext());
            }));
            rowIdManager.delete(750);
            final int[] id = new int[1];
            rowIdManager.add(rowAddress -> {
                id[0] = rowAddress.getId();
                return true;
            });
            assertEquals(750, id[0]);
        } finally {
            for (Integer value : TestUtils.prepareBoundsBatch(lastId, maxIdSize)) {
                new File(filesIdPath + value).delete();
            }
        }
    }

    @Test
    public void streamTest() {
        final int lastId = 750;
        createFiles(lastId);
        rowIdManager = prepareRowIdManager();

        try {
            final AtomicInteger counter = new AtomicInteger();
            rowIdManager.stream(rowAddress -> {
                assertTrue(rowIdManager.process(rowAddress.getId(), rowAddressProcessed -> {
                    assertEquals(rowAddress, rowAddressProcessed);
                }));
                counter.incrementAndGet();
            });
            assertEquals(lastId, counter.get());
        } finally {
            for (Integer value : TestUtils.prepareBoundsBatch(lastId, maxIdSize)) {
                new File(filesIdPath + value).delete();
            }
        }
    }

    private void createFiles(int lastId) {
        TestUtils.createIdFiles(lastId, maxIdSize, compressSize, fileName, filesIdPath, filesRowPath, rowAddressSize, null);
    }

    private RowIdManager prepareRowIdManager() {
        return TestUtils.prepareRowIdManager(maxIdSize, compressSize, fileName, filesIdPath, filesRowPath);
    }

}
