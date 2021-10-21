import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import server.model.*;
import server.model.impl.*;
import server.model.pojo.ICondition;
import server.model.pojo.Row;
import server.model.pojo.SimpleCondition;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class RowRepositoryTest {
    private static final String fileVariablesName = "rowIdVariables";
    private static final String filesIdPath = "rowId";
    private static final String filesRowPath = "row";
    private static final int maxIdSize = 20;
    private static final int compressSize = 2;

    @Parameterized.Parameters
    public static Object[][] data() {
        return new Object[1][0];
    }

    private static IndexService mockIndexService() {
        final IndexService indexService = mock(IndexService.class);
        final ICondition condition1 = new SimpleCondition(ICondition.SimpleType.GT, "int", 20);
        final ICondition condition2 = new SimpleCondition(ICondition.SimpleType.LIKE, "String", "es");
        when(indexService.search(any(ICondition.class), anyInt())).thenAnswer(invocation -> {
            final ICondition condition = (ICondition) invocation.getArguments()[0];
            final Set<Integer> set = new HashSet<>();
            boolean found = false;
            if (condition1.equals(condition)) {
                for (int i = 10; i <= 60; i++) {
                    set.add(i);
                }
                found = true;
            } else if (condition2.equals(condition)) {
                for (int i = 230; i <= 280; i++) {
                    set.add(i);
                }
                found = true;
            }
            return new IndexService.SearchResult(found, set);
        });
        return indexService;
    }

    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(new File(filesIdPath));
        FileUtils.deleteDirectory(new File(filesRowPath));
    }

    @Test
    public void processTest() {
        final int lastId = 750;
        createFiles(lastId);
        final RowRepository rowRepository = prepareRepository(1000);
        TestUtils.doAndSleep(rowRepository, () -> {
            assertFalse(rowRepository.process(0, Assert::assertNull));
            assertTrue(rowRepository.process(1, row -> assertEquals(TestUtils.generateRow(1, 1), row)));
            assertTrue(rowRepository.process(272, row -> assertEquals(TestUtils.generateRow(272, 272), row)));
            assertTrue(rowRepository.process(750, row -> assertEquals(TestUtils.generateRow(750, 750), row)));
            assertFalse(rowRepository.process(751, Assert::assertNull));
        });
    }

    @Test
    public void addTest() {
        addTest(1);
        addTest(2);
        addTest(3);
        addTest(4);
        addTest(1000);
    }

    public void addTest(int bufferSize) {
        int lastId = 750;
        createFiles(lastId);
        final RowRepository rowRepository = prepareRepository(bufferSize);
        TestUtils.doAndSleep(rowRepository, () -> {
            rowRepository.add(TestUtils.generateRow(0, 200));
            assertTrue(rowRepository.process(750, row -> assertEquals(TestUtils.generateRow(750, 750), row)));
            assertTrue(rowRepository.process(751, row -> assertEquals(TestUtils.generateRow(751, 200), row)));
            rowRepository.add(TestUtils.generateRow(0, 5004));
            assertTrue(rowRepository.process(751, row -> assertEquals(TestUtils.generateRow(751, 200), row)));
            assertTrue(rowRepository.process(752, row -> assertEquals(TestUtils.generateRow(752, 5004), row)));
            rowRepository.add(TestUtils.generateRow(751, 7664));
            rowRepository.add(TestUtils.generateRow(752, 6006));
            assertTrue(rowRepository.process(751, row -> assertEquals(TestUtils.generateRow(751, 7664), row)));
            assertTrue(rowRepository.process(752, row -> assertEquals(TestUtils.generateRow(752, 6006), row)));
            rowRepository.add(TestUtils.generateRow(20, 96));
            rowRepository.add(TestUtils.generateRow(21, 97));
            rowRepository.add(TestUtils.generateRow(22, 98));
            assertTrue(rowRepository.process(19, row -> assertEquals(TestUtils.generateRow(19, 19), row)));
            assertTrue(rowRepository.process(20, row -> assertEquals(TestUtils.generateRow(20, 96), row)));
            assertTrue(rowRepository.process(21, row -> assertEquals(TestUtils.generateRow(21, 97), row)));
            assertTrue(rowRepository.process(22, row -> assertEquals(TestUtils.generateRow(22, 98), row)));
            assertTrue(rowRepository.process(751, row -> assertEquals(TestUtils.generateRow(751, 7664), row)));
            assertTrue(rowRepository.process(752, row -> assertEquals(TestUtils.generateRow(752, 6006), row)));
            rowRepository.add(TestUtils.generateRow(0, 753));
            assertTrue(rowRepository.process(19, row -> assertEquals(TestUtils.generateRow(19, 19), row)));
            assertTrue(rowRepository.process(20, row -> assertEquals(TestUtils.generateRow(20, 96), row)));
            assertTrue(rowRepository.process(21, row -> assertEquals(TestUtils.generateRow(21, 97), row)));
            assertTrue(rowRepository.process(22, row -> assertEquals(TestUtils.generateRow(22, 98), row)));
            assertTrue(rowRepository.process(751, row -> assertEquals(TestUtils.generateRow(751, 7664), row)));
            assertTrue(rowRepository.process(752, row -> assertEquals(TestUtils.generateRow(752, 6006), row)));
            assertTrue(rowRepository.process(753, row -> assertEquals(TestUtils.generateRow(753, 753), row)));
        });
    }

    @Test
    public void deleteTest() {
        deleteTest(1);
        deleteTest(2);
        deleteTest(3);
        deleteTest(4);
        deleteTest(1000);
    }

    public void deleteTest(int bufferSize) {
        int lastId = 750;
        createFiles(lastId);
        final RowRepository rowRepository = prepareRepository(bufferSize);
        TestUtils.doAndSleep(rowRepository, () -> {
            assertTrue(rowRepository.process(299, row -> assertEquals(TestUtils.generateRow(299, 299), row)));
            assertTrue(rowRepository.process(300, row -> assertEquals(TestUtils.generateRow(300, 300), row)));
            assertTrue(rowRepository.process(301, row -> assertEquals(TestUtils.generateRow(301, 301), row)));
            rowRepository.delete(300);
            assertTrue(rowRepository.process(299, row -> assertEquals(TestUtils.generateRow(299, 299), row)));
            assertFalse(rowRepository.process(300, row -> assertEquals(TestUtils.generateRow(300, 300), row)));
            assertTrue(rowRepository.process(301, row -> assertEquals(TestUtils.generateRow(301, 301), row)));
            assertTrue(rowRepository.process(749, row -> assertEquals(TestUtils.generateRow(749, 749), row)));
            assertTrue(rowRepository.process(749, row -> assertEquals(TestUtils.generateRow(749, 749), row)));
            assertTrue(rowRepository.process(750, row -> assertEquals(TestUtils.generateRow(750, 750), row)));
            rowRepository.delete(301);
            assertTrue(rowRepository.process(299, row -> assertEquals(TestUtils.generateRow(299, 299), row)));
            assertFalse(rowRepository.process(300, row -> assertEquals(TestUtils.generateRow(300, 300), row)));
            assertFalse(rowRepository.process(301, row -> assertEquals(TestUtils.generateRow(301, 301), row)));
            assertTrue(rowRepository.process(749, row -> assertEquals(TestUtils.generateRow(749, 749), row)));
            assertTrue(rowRepository.process(750, row -> assertEquals(TestUtils.generateRow(750, 750), row)));
            rowRepository.delete(299);
            assertFalse(rowRepository.process(299, row -> assertEquals(TestUtils.generateRow(299, 299), row)));
            assertFalse(rowRepository.process(300, row -> assertEquals(TestUtils.generateRow(300, 300), row)));
            assertFalse(rowRepository.process(301, row -> assertEquals(TestUtils.generateRow(301, 301), row)));
            assertTrue(rowRepository.process(749, row -> assertEquals(TestUtils.generateRow(749, 749), row)));
            assertTrue(rowRepository.process(750, row -> assertEquals(TestUtils.generateRow(750, 750), row)));
            rowRepository.delete(750);
            assertFalse(rowRepository.process(750, row -> assertEquals(TestUtils.generateRow(750, 750), row)));
            rowRepository.add(TestUtils.generateRow(0, 751));
            assertTrue(rowRepository.process(751, row -> assertEquals(TestUtils.generateRow(751, 751), row)));
        });
    }

    @Test
    public void getListTest() {
        getListTest(1);
        getListTest(10);
        getListTest(40);
        getListTest(1000);
    }

    public void getListTest(int bufferSize) {
        int lastId = 750;
        createFiles(lastId);
        final RowRepository rowRepository = prepareRepository(bufferSize);
        TestUtils.doAndSleep(rowRepository, () -> {
            for (int i = 10; i <= 60; i++) {
                final Map<String, Comparable> map = new HashMap<>();
                map.put("int", i - 20);
                rowRepository.add(new Row(i, map));
            }
            assertTrue(rowRepository.process(60, row -> assertEquals(40, row.getFields().get("int"))));
            for (int i = 230; i <= 330; i++) {
                final Map<String, Comparable> map = new HashMap<>();
                map.put("String", "ess");
                rowRepository.add(new Row(i, map));
            }
            assertTrue(rowRepository.process(330, row -> assertEquals("ess", row.getFields().get("String"))));
            {
                final ICondition condition = new SimpleCondition(ICondition.SimpleType.GT, "int", 20);
                List<Row> rows = rowRepository.getList(condition, 0, 60);
                assertEquals(20, rows.size());
                for (int i = 0; i < 20; i++) {
                    final Map<String, Comparable> map = new HashMap<>();
                    map.put("int", i + 21);
                    assertEquals(new Row(i + 41, map), rows.get(i));
                    assertRows(rowRepository, rows, i + 41, i);
                }
                rows = rowRepository.getList(condition, 0, 15);
                assertEquals(15, rows.size());
                for (int i = 0; i < 15; i++) {
                    final Map<String, Comparable> map = new HashMap<>();
                    map.put("int", i + 21);
                    assertEquals(new Row(i + 41, map), rows.get(i));
                    assertRows(rowRepository, rows, i + 41, i);
                }
                rows = rowRepository.getList(condition, 0, 10);
                assertEquals(10, rows.size());
                for (int i = 0; i < 10; i++) {
                    final Map<String, Comparable> map = new HashMap<>();
                    map.put("int", i + 21);
                    assertEquals(new Row(i + 41, map), rows.get(i));
                    assertRows(rowRepository, rows, i + 41, i);
                }
                rows = rowRepository.getList(condition, 10, 10);
                assertEquals(10, rows.size());
                for (int i = 0; i < 10; i++) {
                    final Map<String, Comparable> map = new HashMap<>();
                    map.put("int", i + 31);
                    assertEquals(new Row(i + 51, map), rows.get(i));
                    assertRows(rowRepository, rows, i + 51, i);
                }
            }
            {
                final ICondition condition = new SimpleCondition(ICondition.SimpleType.LIKE, "String", "es");
                List<Row> rows = rowRepository.getList(condition, 0, 60);
                assertEquals(51, rows.size());
                for (int i = 0; i < 51; i++) {
                    final Map<String, Comparable> map = new HashMap<>();
                    map.put("String", "ess");
                    assertEquals(new Row(i + 230, map), rows.get(i));
                    assertRows(rowRepository, rows, i + 230, i);
                }
                rows = rowRepository.getList(condition, 0, 15);
                assertEquals(15, rows.size());
                for (int i = 0; i < 15; i++) {
                    final Map<String, Comparable> map = new HashMap<>();
                    map.put("String", "ess");
                    assertEquals(new Row(i + 230, map), rows.get(i));
                    assertRows(rowRepository, rows, i + 230, i);
                }
                rows = rowRepository.getList(condition, 0, 10);
                assertEquals(10, rows.size());
                for (int i = 0; i < 10; i++) {
                    final Map<String, Comparable> map = new HashMap<>();
                    map.put("String", "ess");
                    assertEquals(new Row(i + 230, map), rows.get(i));
                    assertRows(rowRepository, rows, i + 230, i);
                }
                rows = rowRepository.getList(condition, 10, 10);
                assertEquals(10, rows.size());
                for (int i = 0; i < 10; i++) {
                    final Map<String, Comparable> map = new HashMap<>();
                    map.put("String", "ess");
                    assertEquals(new Row(i + 240, map), rows.get(i));
                    assertRows(rowRepository, rows, i + 240, i);
                }
            }
        });
    }

    @Test
    public void fieldsChangedTest() {
        fieldsChangedTest(1);
        fieldsChangedTest(10);
        fieldsChangedTest(40);
        fieldsChangedTest(1000);
    }

    public void fieldsChangedTest(int bufferSize) {
        int lastId = 750;
        createFiles(lastId);
        final ModelService modelService = new ModelServiceImpl("", true, new ObjectConverterImpl(new DataCompressorImpl()), null);
        try {
            modelService.stop();
            modelService.add("field1", String.class);
            modelService.add("field2", String.class);
            modelService.add("field3", String.class);
            final RowRepository rowRepository = prepareRepository(modelService, bufferSize);
            TestUtils.doAndSleep(rowRepository, () -> {
                for (int i = 1; i <= 750; i++) {
                    final Map<String, Comparable> map = new LinkedHashMap<>();
                    map.put("field1", "1f" + i);
                    map.put("field2", "2f" + i);
                    map.put("field3", "3f" + i);
                    rowRepository.add(new Row(i, map));
                }
                List<Row> rows = rowRepository.getList(ICondition.empty, 0, 750);
                assertEquals(750, rows.size());
                for (int i = 1; i <= 750; i++) {
                    final Map<String, Comparable> map = new LinkedHashMap<>();
                    map.put("field1", "1f" + i);
                    map.put("field2", "2f" + i);
                    map.put("field3", "3f" + i);
                    assertEquals(new Row(i, map), rows.get(i - 1));
                }
                modelService.delete("field4");
                rows = rowRepository.getList(ICondition.empty, 0, 750);
                assertEquals(750, rows.size());
                for (int i = 1; i <= 750; i++) {
                    final Map<String, Comparable> map = new LinkedHashMap<>();
                    map.put("field1", "1f" + i);
                    map.put("field2", "2f" + i);
                    map.put("field3", "3f" + i);
                    assertEquals(new Row(i, map), rows.get(i - 1));
                }
                modelService.delete("field2");
                rows = rowRepository.getList(ICondition.empty, 0, 750);
                assertEquals(750, rows.size());
                for (int i = 1; i <= 750; i++) {
                    final Map<String, Comparable> map = new LinkedHashMap<>();
                    map.put("field1", "1f" + i);
                    map.put("field3", "3f" + i);
                    assertEquals(new Row(i, map), rows.get(i - 1));
                }
                modelService.delete("field1");
                rows = rowRepository.getList(ICondition.empty, 0, 750);
                assertEquals(750, rows.size());
                for (int i = 1; i <= 750; i++) {
                    final Map<String, Comparable> map = new LinkedHashMap<>();
                    map.put("field3", "3f" + i);
                    assertEquals(new Row(i, map), rows.get(i - 1));
                }
                modelService.delete("field3");
                rows = rowRepository.getList(ICondition.empty, 0, 750);
                assertEquals(750, rows.size());
                for (int i = 1; i <= 750; i++) {
                    final Map<String, Comparable> map = new LinkedHashMap<>();
                    assertEquals(new Row(i, map), rows.get(i - 1));
                }
            });
        } finally {
            new File("model").delete();
        }
    }

    @Test
    public void concurrentTest() {
        concurrentTest(1);
        concurrentTest(10);
        concurrentTest(40);
        concurrentTest(1000);
    }

    private void concurrentTest(int bufferSize) {
        int lastId = 250;
        final int max = 100;
        createFiles(lastId);
        final RowRepository rowRepository = prepareRepository(bufferSize);
        TestUtils.doAndSleep(rowRepository, () -> {
            final AtomicInteger count = new AtomicInteger();
            final Thread thread1 = new Thread(() -> {
                for (int i = 0; i < max; i++) {
                    rowRepository.add(TestUtils.generateRow(0, i));
                }
                System.out.println(Thread.currentThread().getName() + " finished");
            });
            final Thread thread2 = new Thread(() -> {
                for (int i = 0; i < max; i++) {
                    rowRepository.delete(i + 100);
                }
                System.out.println(Thread.currentThread().getName() + " finished");
            });
            final Thread destroyThread = new Thread(() -> {
                for (int i = 0; i < max; i++) {
                    rowRepository.destroy();
                }
                System.out.println(Thread.currentThread().getName() + " finished");
            });
            thread1.start();
            thread2.start();
            destroyThread.start();
            for (int i = 0; i < max; i++) {
                rowRepository.process(i, row -> count.incrementAndGet());
            }
            System.out.println("count " + count.get());
            System.out.println();
            count.set(0);
            try {
                thread1.join();
                thread2.join();
                destroyThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for (int i = 0; i < 1000; i++) {
                rowRepository.process(i, row -> count.incrementAndGet());
            }
            System.out.println("count " + count.get());
            System.out.println();
            System.out.println("main thread finished");
        });
    }

    @Test
    public void sizeTest() {
        sizeTest(1);
        sizeTest(10);
        sizeTest(40);
        sizeTest(1000);
    }

    private void sizeTest(int bufferSize) {
        int lastId = 750;
        createFiles(lastId);
        final RowRepository rowRepository = prepareRepository(bufferSize);
        TestUtils.doAndSleep(rowRepository, () -> {
            assertEquals(750, rowRepository.size(ICondition.empty, -1));
            assertEquals(0, rowRepository.size(new SimpleCondition(ICondition.SimpleType.EQ, "testField1", "test"), -1));
            assertEquals(0, rowRepository.size(new SimpleCondition(ICondition.SimpleType.EQ, "testField1", "test3"), -1));
            assertEquals(0, rowRepository.size(new SimpleCondition(ICondition.SimpleType.EQ, "testField2", "test2"), -1));
            assertEquals(0, rowRepository.size(new SimpleCondition(ICondition.SimpleType.EQ, "testField2", "test4"), -1));
            for (int i = 70; i < 120; i++) {
                final Map<String, Comparable> map = new HashMap<>();
                map.put("testField1", "test");
                rowRepository.add(new Row(i, map));
            }
            assertEquals(750, rowRepository.size(ICondition.empty, -1));
            assertEquals(50, rowRepository.size(new SimpleCondition(ICondition.SimpleType.EQ, "testField1", "test"), -1));
            assertEquals(0, rowRepository.size(new SimpleCondition(ICondition.SimpleType.EQ, "testField2", "test2"), -1));
            assertEquals(0, rowRepository.size(new SimpleCondition(ICondition.SimpleType.EQ, "testField1", "test3"), -1));
            assertEquals(0, rowRepository.size(new SimpleCondition(ICondition.SimpleType.EQ, "testField2", "test4"), -1));
            {
                final Map<String, Comparable> map = new HashMap<>();
                map.put("testField1", "test");
                rowRepository.add(new Row(0, map));
            }
            {
                final Map<String, Comparable> map = new HashMap<>();
                map.put("testField1", "test3");
                rowRepository.add(new Row(0, map));
            }
            assertEquals(752, rowRepository.size(ICondition.empty, -1));
            assertEquals(51, rowRepository.size(new SimpleCondition(ICondition.SimpleType.EQ, "testField1", "test"), -1));
            assertEquals(1, rowRepository.size(new SimpleCondition(ICondition.SimpleType.EQ, "testField1", "test3"), -1));
            assertEquals(0, rowRepository.size(new SimpleCondition(ICondition.SimpleType.EQ, "testField2", "test2"), -1));
            assertEquals(0, rowRepository.size(new SimpleCondition(ICondition.SimpleType.EQ, "testField2", "test4"), -1));
            for (int i = 130; i < 180; i++) {
                final Map<String, Comparable> map = new HashMap<>();
                map.put("testField2", "test2");
                rowRepository.add(new Row(i, map));
            }
            assertEquals(752, rowRepository.size(ICondition.empty, -1));
            assertEquals(51, rowRepository.size(new SimpleCondition(ICondition.SimpleType.EQ, "testField1", "test"), -1));
            assertEquals(1, rowRepository.size(new SimpleCondition(ICondition.SimpleType.EQ, "testField1", "test3"), -1));
            assertEquals(50, rowRepository.size(new SimpleCondition(ICondition.SimpleType.EQ, "testField2", "test2"), -1));
            assertEquals(0, rowRepository.size(new SimpleCondition(ICondition.SimpleType.EQ, "testField2", "test4"), -1));
            {
                final Map<String, Comparable> map = new HashMap<>();
                map.put("testField2", "test2");
                rowRepository.add(new Row(0, map));
            }
            {
                final Map<String, Comparable> map = new HashMap<>();
                map.put("testField2", "test4");
                rowRepository.add(new Row(0, map));
            }
            assertEquals(754, rowRepository.size(ICondition.empty, -1));
            assertEquals(51, rowRepository.size(new SimpleCondition(ICondition.SimpleType.EQ, "testField1", "test"), -1));
            assertEquals(1, rowRepository.size(new SimpleCondition(ICondition.SimpleType.EQ, "testField1", "test3"), -1));
            assertEquals(51, rowRepository.size(new SimpleCondition(ICondition.SimpleType.EQ, "testField2", "test2"), -1));
            assertEquals(1, rowRepository.size(new SimpleCondition(ICondition.SimpleType.EQ, "testField2", "test4"), -1));
            assertEquals(18, rowRepository.size(new SimpleCondition(ICondition.SimpleType.EQ, "testField1", "test"), 18));
            assertEquals(40, rowRepository.size(new SimpleCondition(ICondition.SimpleType.EQ, "testField1", "test"), 40));
        });
    }

    private void assertRows(RowRepository rowRepository, final List<Row> rows, final int id, final int index) {
        assertTrue(rowRepository.process(id, row -> assertEquals(row, rows.get(index))));
    }

    private RowRepository prepareRepository(int bufferSize) {
        return prepareRepository(TestUtils.mockModelService(), bufferSize);
    }

    private RowRepository prepareRepository(ModelService modelService, int bufferSize) {
        return new TestRowRepository(bufferSize, new ObjectConverterImpl(new DataCompressorImpl()), new DestroyServiceImpl(1000), modelService);
    }

    private void createFiles(int lastId) {
        final Map<Integer, byte[]> map = TestUtils.createRowMap(lastId);
        Utils.createDirectoryTree(new File(filesIdPath));
        Utils.createDirectoryTree(new File(filesRowPath));
        TestUtils.createRowFiles(map, filesRowPath + "/" + filesRowPath, maxIdSize / compressSize);
        TestUtils.createRowIdFiles(lastId, maxIdSize, compressSize, filesIdPath + "/" + fileVariablesName,
                filesIdPath + "/" + filesIdPath, filesRowPath + "/" + filesRowPath, 0, map);
    }

    private static class TestRowRepository extends RowRepositoryImpl {
        TestRowRepository(int bufferSize, ObjectConverter objectConverter, DestroyService destroyService, ModelService modelService) {
            super("", true, new ObjectConverterImpl(new DataCompressorImpl()), destroyService, TestUtils.prepareRowIdRepository("", destroyService, maxIdSize, compressSize, objectConverter), new FileHelperImpl(), mockIndexService(), new ConditionServiceImpl(TestUtils.mockModelService()), modelService, bufferSize);
        }

        @Override
        public void destroy() {
            super.destroy();
            rowIdRepository.destroy();
        }

        @Override
        public void stop() {
            super.stop();
            rowIdRepository.stop();
        }
    }
}
