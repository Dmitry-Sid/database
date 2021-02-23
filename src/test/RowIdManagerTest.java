import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sample.model.ObjectConverter;
import sample.model.ObjectConverterImpl;
import sample.model.RowIdManager;
import sample.model.RowIdManagerImpl;
import sample.model.pojo.RowAddress;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class RowIdManagerTest {

    private RowIdManager rowIdManager;
    private static final String fileName = "rowIdVariables.test";
    private static final String filesIdPath = "rowId";
    private static final String filesRowPath = "row";
    private static final int maxSize = 500;
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
    public void newIdTest() {
        assertEquals(1, rowIdManager.newId());
    }

    @Test
    public void processTest() {
        final int lastId = 750;
        createFiles(lastId);
        rowIdManager = prepareRowIdManager();
        assertFalse(rowIdManager.process(0, rowAddress -> {
        }));
        assertTrue(rowIdManager.process(1, rowAddress -> {
            assertEquals(filesRowPath + 1, rowAddress.getFilePath());
            assertEquals(1, rowAddress.getId());
            assertEquals(1, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            final RowAddress rowAddressNext = new RowAddress(filesRowPath + 1, 2, 6, rowAddressSize);
            assertEquals(rowAddressNext, rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(250, rowAddress -> {
            assertEquals(filesRowPath + 1, rowAddress.getFilePath());
            assertEquals(250, rowAddress.getId());
            assertEquals(1246, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            final RowAddress rowAddressNext = new RowAddress(filesRowPath + 1, 251, 1251, rowAddressSize);
            assertEquals(rowAddressNext, rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(499, rowAddress -> {
            assertEquals(filesRowPath + 1, rowAddress.getFilePath());
            assertEquals(499, rowAddress.getId());
            assertEquals(2491, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            final RowAddress rowAddressNext = new RowAddress(filesRowPath + 1, 500, 2496, rowAddressSize);
            assertEquals(rowAddressNext, rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(500, rowAddress -> {
            assertEquals(filesRowPath + 1, rowAddress.getFilePath());
            assertEquals(500, rowAddress.getId());
            assertEquals(2496, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            assertNull(rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(501, rowAddress -> {
            assertEquals(filesRowPath + 2, rowAddress.getFilePath());
            assertEquals(501, rowAddress.getId());
            assertEquals(1, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            final RowAddress rowAddressNext = new RowAddress(filesRowPath + 2, 502, 6, rowAddressSize);
            assertEquals(rowAddressNext, rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(749, rowAddress -> {
            assertEquals(filesRowPath + 2, rowAddress.getFilePath());
            assertEquals(749, rowAddress.getId());
            assertEquals(1241, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            final RowAddress rowAddressNext = new RowAddress(filesRowPath + 2, 750, 1246, rowAddressSize);
            assertEquals(rowAddressNext, rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(750, rowAddress -> {
            assertEquals(filesRowPath + 2, rowAddress.getFilePath());
            assertEquals(750, rowAddress.getId());
            assertEquals(1246, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            assertNull(rowAddress.getNext());
        }));
        assertFalse(rowIdManager.process(751, rowAddress -> {
        }));
        assertEquals(751, rowIdManager.newId());
        for (Map.Entry<RowIdManagerImpl.IdBounds, String> entry : prepareBoundsMap(lastId).entrySet()) {
            new File(entry.getValue()).delete();
        }
    }

    @Test
    public void transformTest() {
        final int lastId = 750;
        createFiles(lastId);
        rowIdManager = prepareRowIdManager();
        rowIdManager.transform(501, 14);
        rowIdManager.transform(250, 7);
        assertTrue(rowIdManager.process(501, rowAddress -> {
            assertEquals(filesRowPath + 2, rowAddress.getFilePath());
            assertEquals(501, rowAddress.getId());
            assertEquals(1, rowAddress.getPosition());
            assertEquals(14, rowAddress.getSize());
            final RowAddress rowAddressNext = new RowAddress(filesRowPath + 2, 502, 15, rowAddressSize);
            assertEquals(rowAddressNext, rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(650, rowAddress -> {
            assertEquals(filesRowPath + 2, rowAddress.getFilePath());
            assertEquals(650, rowAddress.getId());
            assertEquals(755, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            final RowAddress rowAddressNext = new RowAddress(filesRowPath + 2, 651, 760, rowAddressSize);
            assertEquals(rowAddressNext, rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(750, rowAddress -> {
            assertEquals(filesRowPath + 2, rowAddress.getFilePath());
            assertEquals(750, rowAddress.getId());
            assertEquals(1255, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            assertNull(rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(1, rowAddress -> {
            assertEquals(filesRowPath + 1, rowAddress.getFilePath());
            assertEquals(1, rowAddress.getId());
            assertEquals(1, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            final RowAddress rowAddressNext = new RowAddress(filesRowPath + 1, 2, 6, rowAddressSize);
            assertEquals(rowAddressNext, rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(250, rowAddress -> {
            assertEquals(filesRowPath + 1, rowAddress.getFilePath());
            assertEquals(250, rowAddress.getId());
            assertEquals(1246, rowAddress.getPosition());
            assertEquals(7, rowAddress.getSize());
            final RowAddress rowAddressNext = new RowAddress(filesRowPath + 1, 251, 1253, rowAddressSize);
            assertEquals(rowAddressNext, rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(499, rowAddress -> {
            assertEquals(filesRowPath + 1, rowAddress.getFilePath());
            assertEquals(499, rowAddress.getId());
            assertEquals(2493, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            final RowAddress rowAddressNext = new RowAddress(filesRowPath + 1, 500, 2498, rowAddressSize);
            assertEquals(rowAddressNext, rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(500, rowAddress -> {
            assertEquals(filesRowPath + 1, rowAddress.getFilePath());
            assertEquals(500, rowAddress.getId());
            assertEquals(2498, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            assertNull(rowAddress.getNext());
        }));
        for (Map.Entry<RowIdManagerImpl.IdBounds, String> entry : prepareBoundsMap(lastId).entrySet()) {
            new File(entry.getValue()).delete();
        }
    }

    @Test
    public void addTest() {
        final int lastId = 750;
        createFiles(lastId);
        rowIdManager = prepareRowIdManager();
        String exceptionStr = null;
        try {
            rowIdManager.add(250, "row1", null);
        } catch (Exception e) {
            exceptionStr = e.getMessage();
        }
        assertEquals("already has same id : " + 250, exceptionStr);
        try {
            rowIdManager.add(573, "row1", null);
        } catch (Exception e) {
            exceptionStr = e.getMessage();
        }
        assertEquals("already has same id : " + 573, exceptionStr);
        rowIdManager.add(751, filesRowPath + 3, rowAddress -> {
            rowAddress.setPosition(1251);
            rowAddress.setSize(10);
        });
        assertEquals(752, rowIdManager.newId());
        assertTrue(rowIdManager.process(250, rowAddress -> {
            assertEquals(filesRowPath + 1, rowAddress.getFilePath());
            assertEquals(250, rowAddress.getId());
            assertEquals(1246, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            final RowAddress rowAddressNext = new RowAddress(filesRowPath + 1, 251, 1251, rowAddressSize);
            assertEquals(rowAddressNext, rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(750, rowAddress -> {
            assertEquals(filesRowPath + 2, rowAddress.getFilePath());
            assertEquals(750, rowAddress.getId());
            assertEquals(1246, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            final RowAddress rowAddressNext = new RowAddress(filesRowPath + 3, 751, 1251, 10);
            assertEquals(rowAddressNext, rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(751, rowAddress -> {
            assertEquals(filesRowPath + 3, rowAddress.getFilePath());
            assertEquals(751, rowAddress.getId());
            assertEquals(1251, rowAddress.getPosition());
            assertEquals(10, rowAddress.getSize());
            assertNull(rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(250, rowAddress -> {
            assertEquals(filesRowPath + 1, rowAddress.getFilePath());
            assertEquals(250, rowAddress.getId());
            assertEquals(1246, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            final RowAddress rowAddressNext = new RowAddress(filesRowPath + 1, 251, 1251, rowAddressSize);
            assertEquals(rowAddressNext, rowAddress.getNext());
        }));
        try {
            rowIdManager.add(751, "row1", null);
        } catch (Exception e) {
            exceptionStr = e.getMessage();
        }
        assertEquals("already has same id : " + 751, exceptionStr);
        rowIdManager.add(1200, filesRowPath + 4, rowAddress -> {
            rowAddress.setPosition(0);
            rowAddress.setSize(67);
        });
        assertEquals(1201, rowIdManager.newId());
        assertTrue(rowIdManager.process(250, rowAddress -> {
            assertEquals(filesRowPath + 1, rowAddress.getFilePath());
            assertEquals(250, rowAddress.getId());
            assertEquals(1246, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            final RowAddress rowAddressNext = new RowAddress(filesRowPath + 1, 251, 1251, rowAddressSize);
            assertEquals(rowAddressNext, rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(750, rowAddress -> {
            assertEquals(filesRowPath + 2, rowAddress.getFilePath());
            assertEquals(750, rowAddress.getId());
            assertEquals(1246, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            final RowAddress rowAddressNext = new RowAddress(filesRowPath + 3, 751, 1251, 10);
            assertEquals(rowAddressNext, rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(751, rowAddress -> {
            assertEquals(filesRowPath + 3, rowAddress.getFilePath());
            assertEquals(751, rowAddress.getId());
            assertEquals(1251, rowAddress.getPosition());
            assertEquals(10, rowAddress.getSize());
            assertNull(rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(250, rowAddress -> {
            assertEquals(filesRowPath + 1, rowAddress.getFilePath());
            assertEquals(250, rowAddress.getId());
            assertEquals(1246, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            final RowAddress rowAddressNext = new RowAddress(filesRowPath + 1, 251, 1251, rowAddressSize);
            assertEquals(rowAddressNext, rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(1200, rowAddress -> {
            assertEquals(filesRowPath + 4, rowAddress.getFilePath());
            assertEquals(1200, rowAddress.getId());
            assertEquals(0, rowAddress.getPosition());
            assertEquals(67, rowAddress.getSize());
            assertNull(rowAddress.getNext());
        }));
        for (Map.Entry<RowIdManagerImpl.IdBounds, String> entry : prepareBoundsMap(1200).entrySet()) {
            new File(entry.getValue()).delete();
        }
    }

    private RowIdManager prepareRowIdManager() {
        return new RowIdManagerImpl(new ObjectConverterImpl(), maxSize, fileName, filesIdPath, filesRowPath);
    }

    private void createFiles(int lastId) {
        final Map<RowIdManagerImpl.IdBounds, String> boundsMap = prepareBoundsMap(lastId);
        final ObjectConverter objectConverter = new ObjectConverterImpl();
        RowIdManagerImpl.CachedRowAddresses cachedRowAddresses = null;
        int id = 1;
        int batchCount = 1;
        for (Map.Entry<RowIdManagerImpl.IdBounds, String> entry : boundsMap.entrySet()) {
            final Map<Integer, RowAddress> rowAddressMap = new ConcurrentHashMap<>();
            int lastEndPosition = 1;
            RowAddress rowAddress;
            RowAddress rowAddressPrevious = null;
            final int max = Math.min(entry.getKey().highBound, lastId);
            for (int i = entry.getKey().lowBound; i <= max; i++) {
                rowAddress = new RowAddress(filesRowPath + batchCount, id, lastEndPosition, rowAddressSize);
                lastEndPosition += rowAddressSize;
                if (rowAddressPrevious != null) {
                    rowAddressPrevious.setNext(rowAddress);
                }
                rowAddressPrevious = rowAddress;
                rowAddressMap.put(id, rowAddress);
                id++;
            }
            objectConverter.toFile((Serializable) rowAddressMap, entry.getValue());
            cachedRowAddresses = new RowIdManagerImpl.CachedRowAddresses(entry.getValue(), rowAddressMap);
            batchCount++;
        }
        final RowIdManagerImpl.Variables variables = new RowIdManagerImpl.Variables(new AtomicInteger(lastId), boundsMap, cachedRowAddresses);
        objectConverter.toFile(variables, fileName);
    }

    private Map<RowIdManagerImpl.IdBounds, String> prepareBoundsMap(int lastId) {
        final Map<RowIdManagerImpl.IdBounds, String> map = new HashMap<>();
        for (int i = 0; i <= 1 + lastId / maxSize; i++) {
            map.put(new RowIdManagerImpl.IdBounds(i * maxSize + 1, (i + 1) * maxSize), filesIdPath + (i + 1));
        }
        return map;
    }

}
