import org.junit.After;
import org.junit.Test;
import sample.model.*;
import sample.model.pojo.ICondition;
import sample.model.pojo.Row;
import sample.model.pojo.SimpleCondition;

import java.io.File;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
            repository.add(TestUtils.generateRow(751, 7664));
            assertEquals(TestUtils.generateRow(751, 7664), repository.get(751));
            assertEquals(TestUtils.generateRow(752, 5004), repository.get(752));
            repository.add(TestUtils.generateRow(20, 96));
            assertEquals(TestUtils.generateRow(19, 19), repository.get(19));
            assertEquals(TestUtils.generateRow(20, 96), repository.get(20));
            assertEquals(TestUtils.generateRow(21, 21), repository.get(21));
            assertEquals(TestUtils.generateRow(22, 22), repository.get(22));
            repository.add(TestUtils.generateRow(0, 753));
            assertEquals(TestUtils.generateRow(751, 7664), repository.get(751));
            assertEquals(TestUtils.generateRow(752, 5004), repository.get(752));
            assertEquals(TestUtils.generateRow(753, 753), repository.get(753));
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
    public void deleteTest() {
        int lastId = 750;
        try {
            createFiles(lastId);
            final Repository repository = prepareRepository();
            lastId++;
            assertEquals(TestUtils.generateRow(299, 299), repository.get(299));
            assertEquals(TestUtils.generateRow(300, 300), repository.get(300));
            assertEquals(TestUtils.generateRow(301, 301), repository.get(301));
            repository.delete(300);
            assertEquals(TestUtils.generateRow(299, 299), repository.get(299));
            assertNull(repository.get(300));
            assertEquals(TestUtils.generateRow(301, 301), repository.get(301));
            assertEquals(TestUtils.generateRow(749, 749), repository.get(749));
            assertEquals(TestUtils.generateRow(750, 750), repository.get(750));
            repository.delete(750);
            assertNull(repository.get(750));
            repository.add(TestUtils.generateRow(0, 750));
            assertEquals(TestUtils.generateRow(750, 750), repository.get(750));
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
    public void getListTest() {
        int lastId = 750;
        try {
            createFiles(lastId);
            final Repository repository = prepareRepository();
            for (int i = 10; i <= 60; i++) {
                final Map<String, Comparable> map = new HashMap<>();
                map.put("int", i - 20);
                repository.add(new Row(i, map));
            }
            assertEquals(40, repository.get(60).getFields().get("int"));
            for (int i = 230; i <= 330; i++) {
                final Map<String, Comparable> map = new HashMap<>();
                map.put("String", "ess");
                repository.add(new Row(i, map));
            }
            assertEquals("ess", repository.get(330).getFields().get("String"));
            {
                final ICondition condition = new SimpleCondition(ICondition.SimpleType.GT, "int", 20);
                List<Row> rows = repository.getList(condition, 0, 60);
                assertEquals(20, rows.size());
                for (int i = 0; i < 20; i++) {
                    final Map<String, Comparable> map = new HashMap<>();
                    map.put("int", i + 21);
                    assertEquals(new Row(i + 41, map), rows.get(i));
                    assertEquals(repository.get(i + 41), rows.get(i));
                }
                rows = repository.getList(condition, 0, 15);
                assertEquals(15, rows.size());
                for (int i = 0; i < 15; i++) {
                    final Map<String, Comparable> map = new HashMap<>();
                    map.put("int", i + 21);
                    assertEquals(new Row(i + 41, map), rows.get(i));
                    assertEquals(repository.get(i + 41), rows.get(i));
                }
                rows = repository.getList(condition, 0, 10);
                assertEquals(10, rows.size());
                for (int i = 0; i < 10; i++) {
                    final Map<String, Comparable> map = new HashMap<>();
                    map.put("int", i + 21);
                    assertEquals(new Row(i + 41, map), rows.get(i));
                    assertEquals(repository.get(i + 41), rows.get(i));
                }
                rows = repository.getList(condition, 10, 10);
                assertEquals(10, rows.size());
                for (int i = 0; i < 10; i++) {
                    final Map<String, Comparable> map = new HashMap<>();
                    map.put("int", i + 31);
                    assertEquals(new Row(i + 51, map), rows.get(i));
                    assertEquals(repository.get(i + 51), rows.get(i));
                }
            }
            {
                final ICondition condition = new SimpleCondition(ICondition.SimpleType.LIKE, "String", "es");
                List<Row> rows = repository.getList(condition, 0, 60);
                assertEquals(51, rows.size());
                for (int i = 0; i < 51; i++) {
                    final Map<String, Comparable> map = new HashMap<>();
                    map.put("String", "ess");
                    assertEquals(new Row(i + 230, map), rows.get(i));
                    assertEquals(repository.get(i + 230), rows.get(i));
                }
                rows = repository.getList(condition, 0, 15);
                assertEquals(15, rows.size());
                for (int i = 0; i < 15; i++) {
                    final Map<String, Comparable> map = new HashMap<>();
                    map.put("String", "ess");
                    assertEquals(new Row(i + 230, map), rows.get(i));
                    assertEquals(repository.get(i + 230), rows.get(i));
                }
                rows = repository.getList(condition, 0, 10);
                assertEquals(10, rows.size());
                for (int i = 0; i < 10; i++) {
                    final Map<String, Comparable> map = new HashMap<>();
                    map.put("String", "ess");
                    assertEquals(new Row(i + 230, map), rows.get(i));
                    assertEquals(repository.get(i + 230), rows.get(i));
                }
                rows = repository.getList(condition, 10, 10);
                assertEquals(10, rows.size());
                for (int i = 0; i < 10; i++) {
                    final Map<String, Comparable> map = new HashMap<>();
                    map.put("String", "ess");
                    assertEquals(new Row(i + 240, map), rows.get(i));
                    assertEquals(repository.get(i + 240), rows.get(i));
                }
            }
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
        final RowIdManager rowIdManager = TestUtils.prepareRowIdManager(maxIdSize, compressSize, fileVariablesName, filesIdPath, filesRowPath);
        return new RepositoryImpl(new ObjectConverterImpl(), rowIdManager, new FileHelperImpl(), mockIndexService(), new ConditionServiceImpl(mockModelService()));
    }

    private IndexService mockIndexService() {
        final IndexService indexService = mock(IndexService.class);
        final ICondition condition1 = new SimpleCondition(ICondition.SimpleType.GT, "int", 20);
        final ICondition condition2 = new SimpleCondition(ICondition.SimpleType.LIKE, "String", "es");
        when(indexService.search(any(ICondition.class))).thenAnswer(invocation -> {
            final ICondition condition = (ICondition) invocation.getArguments()[0];
            final Set<Integer> set = new HashSet<>();
            if (condition1.equals(condition)) {
                for (int i = 10; i <= 60; i++) {
                    set.add(i);
                }
            } else if (condition2.equals(condition)) {
                for (int i = 230; i <= 280; i++) {
                    set.add(i);
                }
            }
            return new IndexService.SearchResult(true, set);
        });
        return indexService;
    }

    private ModelService mockModelService() {
        return new ModelServiceImpl();
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