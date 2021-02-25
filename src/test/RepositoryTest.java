import org.junit.After;
import org.junit.Test;
import sample.model.*;

import java.io.File;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RepositoryTest {
    private static final String fileVariablesName = "rowIdVariables.test";
    private static final String filesIdPath = "rowId";
    private static final String filesRowPath = "row";
    private static final int maxIdSize = 500;
    private static final int compressSize = 2;

    @After
    public void after() {
        new File(fileVariablesName).delete();
    }

    @Test
    public void getTest() {
        final int lastId = 750;
        try {
            createFiles(lastId);
            final Repository repository = prepareRepository();
            assertNull(repository.get(0));
            assertEquals(TestUtils.generateRow(1, 1), repository.get(1));
            assertEquals(TestUtils.generateRow(272, 272), repository.get(272));
            assertEquals(TestUtils.generateRow(750, 750), repository.get(750));
            assertNull(repository.get(751));
        } finally {
            for (Integer value : TestUtils.prepareBoundsBatch(lastId, maxIdSize)) {
                new File(filesIdPath + value).delete();
            }
            String lastFileName = null;
            for (Map.Entry<Integer, byte[]> entry : TestUtils.createRowMap(lastId).entrySet()) {
                final String fileName = filesRowPath + TestUtils.getRowFileNumber(entry.getKey(), maxIdSize / compressSize);
                if (!fileName.equals(lastFileName)) {
                    lastFileName = fileName;
                    new File(fileName).delete();
                }
            }
        }
    }

    @Test
    public void addTest() {
        int lastId = 750;
        try {
            createFiles(lastId);
            final Repository repository = prepareRepository();
            lastId++;
            repository.add(TestUtils.generateRow(0, 200));
            assertEquals(TestUtils.generateRow(750, 750), repository.get(750));
            assertEquals(TestUtils.generateRow(751, 200), repository.get(751));
            repository.add(TestUtils.generateRow(0, 5004));
            assertEquals(TestUtils.generateRow(751, 200), repository.get(751));
            assertEquals(TestUtils.generateRow(752, 5004), repository.get(752));
        } finally {
            for (Integer value : TestUtils.prepareBoundsBatch(lastId, maxIdSize)) {
                new File(filesIdPath + value).delete();
            }
            String lastFileName = null;
            for (Map.Entry<Integer, byte[]> entry : TestUtils.createRowMap(lastId).entrySet()) {
                final String fileName = filesRowPath + TestUtils.getRowFileNumber(entry.getKey(), maxIdSize / compressSize);
                if (!fileName.equals(lastFileName)) {
                    lastFileName = fileName;
                    new File(fileName).delete();
                }
            }
        }
    }

    private Repository prepareRepository() {
        return new RepositoryImpl(new ObjectConverterImpl(),
                TestUtils.prepareRowIdManager(maxIdSize, compressSize, fileVariablesName, filesIdPath, filesRowPath), new FileHelperImpl());
    }

    private void createFiles(int lastId) {
        final Map<Integer, byte[]> map = TestUtils.createRowMap(lastId);
        TestUtils.createRowFiles(map, filesRowPath, maxIdSize / compressSize);
        TestUtils.createIdFiles(lastId, maxIdSize, compressSize, fileVariablesName, filesIdPath, filesRowPath, 0, map);
    }

    private RowIdManager prepareRowIdManager() {
        return TestUtils.prepareRowIdManager(maxIdSize, compressSize, fileVariablesName, filesIdPath, filesRowPath);
    }

}