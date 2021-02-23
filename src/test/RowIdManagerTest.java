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
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class RowIdManagerTest {

    private RowIdManager rowIdManager;
    private static final String fileName = "rowIdVariables.test";
    private static final String filesPath = "rowId";
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
        new File(filesPath + 1).delete();
    }

    @Test
    public void newIdTest() {
        assertEquals(1, rowIdManager.newId());
        assertEquals(2, rowIdManager.newId());
        assertEquals(3, rowIdManager.newId());
    }

    @Test
    public void processTest() {
        final int lastId = 750;
        createFiles(lastId);
        rowIdManager = prepareRowIdManager();
        assertFalse(rowIdManager.process(0, rowAddress -> {
        }));
        assertTrue(rowIdManager.process(1, rowAddress -> {
            assertEquals(filesPath + 1, rowAddress.getFilePath());
            assertEquals(1, rowAddress.getId());
            assertEquals(1, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            final RowAddress rowAddressNext = new RowAddress(filesPath + 1, 2, 6, rowAddressSize);
            assertEquals(rowAddressNext, rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(250, rowAddress -> {
            assertEquals(filesPath + 1, rowAddress.getFilePath());
            assertEquals(250, rowAddress.getId());
            assertEquals(1246, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            final RowAddress rowAddressNext = new RowAddress(filesPath + 1, 251, 1251, rowAddressSize);
            assertEquals(rowAddressNext, rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(499, rowAddress -> {
            assertEquals(filesPath + 1, rowAddress.getFilePath());
            assertEquals(499, rowAddress.getId());
            assertEquals(2491, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            final RowAddress rowAddressNext = new RowAddress(filesPath + 1, 500, 2496, rowAddressSize);
            assertEquals(rowAddressNext, rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(500, rowAddress -> {
            assertEquals(filesPath + 1, rowAddress.getFilePath());
            assertEquals(500, rowAddress.getId());
            assertEquals(2496, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            assertNull(rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(501, rowAddress -> {
            assertEquals(filesPath + 2, rowAddress.getFilePath());
            assertEquals(501, rowAddress.getId());
            assertEquals(1, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            final RowAddress rowAddressNext = new RowAddress(filesPath + 2, 502, 6, rowAddressSize);
            assertEquals(rowAddressNext, rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(749, rowAddress -> {
            assertEquals(filesPath + 2, rowAddress.getFilePath());
            assertEquals(749, rowAddress.getId());
            assertEquals(1241, rowAddress.getPosition());
            assertEquals(rowAddressSize, rowAddress.getSize());
            final RowAddress rowAddressNext = new RowAddress(filesPath + 2, 750, 1246, rowAddressSize);
            assertEquals(rowAddressNext, rowAddress.getNext());
        }));
        assertTrue(rowIdManager.process(750, rowAddress -> {
            assertEquals(filesPath + 2, rowAddress.getFilePath());
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

    private RowIdManager prepareRowIdManager() {
        return new RowIdManagerImpl(new ObjectConverterImpl(), maxSize, fileName, filesPath);
    }

    private void createFiles(int lastId) {
        final Map<RowIdManagerImpl.IdBounds, String> boundsMap = prepareBoundsMap(lastId);
        final ObjectConverter objectConverter = new ObjectConverterImpl();
        final AtomicReference<RowIdManagerImpl.CachedRowAddressMap> atomicReference = new AtomicReference<>();
        int id = 1;
        for (Map.Entry<RowIdManagerImpl.IdBounds, String> entry : boundsMap.entrySet()) {
            final Map<Integer, RowAddress> rowAddressMap = new ConcurrentHashMap<>();
            int lastEndPosition = 1;
            RowAddress rowAddress;
            RowAddress rowAddressPrevious = null;
            final int max = Math.min(entry.getKey().highBound, lastId);
            for (int i = entry.getKey().lowBound; i <= max; i++) {
                rowAddress = new RowAddress(entry.getValue(), id, lastEndPosition, rowAddressSize);
                lastEndPosition += rowAddressSize;
                if (rowAddressPrevious != null) {
                    rowAddressPrevious.setNext(rowAddress);
                }
                rowAddressPrevious = rowAddress;
                rowAddressMap.put(id, rowAddress);
                id++;
            }
            objectConverter.toFile((Serializable) rowAddressMap, entry.getValue());
            atomicReference.set(new RowIdManagerImpl.CachedRowAddressMap(entry.getValue(), rowAddressMap));
        }
        final RowIdManagerImpl.Variables variables = new RowIdManagerImpl.Variables(new AtomicInteger(lastId), boundsMap, atomicReference);
        objectConverter.toFile(variables, fileName);

    }

    private Map<RowIdManagerImpl.IdBounds, String> prepareBoundsMap(int lastId) {
        final Map<RowIdManagerImpl.IdBounds, String> map = new HashMap<>();
        for (int i = 0; i <= 1 + lastId / maxSize; i++) {
            map.put(new RowIdManagerImpl.IdBounds(i * maxSize + 1, (i + 1) * maxSize), filesPath + (i + 1));
        }
        return map;
    }

}
