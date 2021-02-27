import sample.model.ObjectConverter;
import sample.model.ObjectConverterImpl;
import sample.model.RowIdManager;
import sample.model.RowIdManagerImpl;
import sample.model.pojo.Row;
import sample.model.pojo.RowAddress;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TestUtils {
    public static Row generateRow(int id, int intValue) {
        return new Row(id, new HashMap<String, Comparable>() {{
            put("field" + intValue, intValue);
        }});
    }

    public static RowIdManager prepareRowIdManager(int maxIdSize, int compressSize, String fileName, String filesIdPath, String filesRowPath) {
        return new RowIdManagerImpl(new ObjectConverterImpl(), maxIdSize, compressSize, fileName, filesIdPath, filesRowPath);
    }

    public static void createIdFiles(int lastId, int maxIdSize, int compressSize, String fileName, String filesIdPath, String filesRowPath, int rowAddressSize, Map<Integer, byte[]> rowMap) {
        final int maxRowSize = maxIdSize / compressSize;
        final Set<Integer> boundsBatch = prepareBoundsBatch(lastId, maxIdSize);
        final ObjectConverter objectConverter = new ObjectConverterImpl();
        RowIdManagerImpl.CachedRowAddresses cachedRowAddresses = null;
        int id = 1;
        int lastRowFileNumber = 1;
        for (Integer value : boundsBatch) {
            final Map<Integer, RowAddress> rowAddressMap = new ConcurrentHashMap<>();
            int lastEndPosition = 0;
            RowAddress rowAddress;
            RowAddress rowAddressPrevious = null;
            final int max = Math.min(value * maxIdSize, lastId);
            for (int i = (value - 1) * maxIdSize + 1; i <= max; i++) {
                final int rowFileNumber = getRowFileNumber(id, maxRowSize);
                if (lastRowFileNumber != rowFileNumber) {
                    lastRowFileNumber = rowFileNumber;
                    lastEndPosition = 0;
                    rowAddressPrevious = null;
                }
                rowAddress = new RowAddress(filesRowPath + getRowFileNumber(id, maxRowSize), id, lastEndPosition, rowMap == null ? rowAddressSize : rowMap.get(id).length);
                lastEndPosition += rowAddress.getSize();
                if (rowAddressPrevious != null) {
                    rowAddressPrevious.setNext(rowAddress);
                    rowAddress.setPrevious(rowAddressPrevious);
                }
                rowAddressPrevious = rowAddress;
                rowAddressMap.put(id, rowAddress);
                id++;
            }
            objectConverter.toFile((Serializable) rowAddressMap, filesIdPath + value);
            cachedRowAddresses = new RowIdManagerImpl.CachedRowAddresses(filesIdPath + value, rowAddressMap);
        }
        final RowIdManagerImpl.Variables variables = new RowIdManagerImpl.Variables(new AtomicInteger(lastId), boundsBatch, cachedRowAddresses);
        objectConverter.toFile(variables, fileName);
    }

    public static Set<Integer> prepareBoundsBatch(int lastId, int maxIdSize) {
        final Set<Integer> set = Collections.synchronizedSet(new LinkedHashSet<>());
        for (int i = 0; i <= lastId / maxIdSize; i++) {
            set.add(i + 1);
        }
        return set;
    }

    public static int getRowIdFileNumber(int id, int maxIdSize) {
        return 1 + (id - 1) / maxIdSize;
    }

    public static int getRowFileNumber(int id, int maxRowSize) {
        return 1 + (id - 1) / maxRowSize;
    }

    public static Map<Integer, byte[]> createRowMap(int lastId) {
        final ObjectConverter objectConverter = new ObjectConverterImpl();
        final Map<Integer, byte[]> map = new HashMap<>();
        for (int i = 1; i <= lastId; i++) {
            map.put(i, objectConverter.toBytes(generateRow(i, i)));
        }
        return map;
    }

    public static void createRowFiles(Map<Integer, byte[]> map, String rowFileName, int maxRowSize) {
        try {
            String lastFileName = null;
            FileOutputStream fileOutputStream = null;
            for (Map.Entry<Integer, byte[]> entry : map.entrySet()) {
                final String fileName = rowFileName + getRowFileNumber(entry.getKey(), maxRowSize);
                if (!fileName.equals(lastFileName)) {
                    if (fileOutputStream != null) {
                        fileOutputStream.close();
                    }
                    fileOutputStream = new FileOutputStream(fileName);
                }

                lastFileName = fileName;
                fileOutputStream.write(entry.getValue());
            }
            if (fileOutputStream != null) {
                fileOutputStream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
