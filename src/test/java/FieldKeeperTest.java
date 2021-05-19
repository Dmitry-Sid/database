import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import server.model.FieldKeeper;
import server.model.impl.BPlusTree;
import server.model.pojo.ICondition;
import server.model.pojo.SimpleCondition;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public abstract class FieldKeeperTest {

    @Before
    public void delete() {
        new File("test.int").delete();
    }

    @After
    public void after() {
        prepareFieldKeeper(Integer.class, "int").clear();
        prepareFieldKeeper(Integer.class, "String").clear();
    }

    @Test
    public void safeStructureTest() {
        final FieldKeeper<Integer, Integer> fieldKeeper = prepareFieldKeeper(Integer.class, "int");
        final int size;
        if (fieldKeeper instanceof BPlusTree) {
            size = Math.max(100, ((BPlusTree<Integer, Integer>) fieldKeeper).treeFactor * 20);
        } else {
            size = 1000;
        }
        for (int i = 0; i < size; i++) {
            fieldKeeper.insert(i, i);
            if (i % 2 == 0) {
                fieldKeeper.insert(i, i + 1000);
            }
        }
        for (int i = 0; i < size; i++) {
            final List<Integer> list = new ArrayList(fieldKeeper.search(i));
            list.sort(Integer::compareTo);
            if (i % 2 == 0) {
                assertEquals(Arrays.asList(i, i + 1000), list);
            } else {
                assertEquals(Collections.singletonList(i), list);
            }
        }
        for (int i = 0; i < size; i++) {
            if (i % 2 == 0) {
                {
                    final FieldKeeper.DeleteResult deleteResult = fieldKeeper.delete(i, i);
                    assertTrue(deleteResult.deleted);
                    assertFalse(deleteResult.fully);
                }
                assertEquals(Collections.singletonList(i + 1000), new ArrayList<>(fieldKeeper.search(i)));
                {
                    final FieldKeeper.DeleteResult deleteResult = fieldKeeper.delete(i, i + 1000);
                    assertTrue(deleteResult.deleted);
                    assertTrue(deleteResult.fully);
                }
                assertEquals(Collections.emptySet(), fieldKeeper.search(i));
            } else {
                {
                    final FieldKeeper.DeleteResult deleteResult = fieldKeeper.delete(i, i);
                    assertTrue(deleteResult.deleted);
                    assertTrue(deleteResult.fully);
                }
                assertEquals(Collections.emptySet(), fieldKeeper.search(i));
            }
        }
    }

    @Test
    public void insertTest() {
        final FieldKeeper<Integer, Integer> fieldKeeper = prepareFieldKeeper(Integer.class, "int");
        assertEquals(Collections.emptySet(), fieldKeeper.search(10));
        fieldKeeper.insert(10, 1);
        fieldKeeper.insert(10, 2);
        fieldKeeper.insert(null, 121);
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(10));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(1, 2), list);
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search((Integer) null));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(121), list);
        }
        fieldKeeper.insert(11, 3);
        fieldKeeper.insert(10, 3);
        fieldKeeper.insert(9, 4);
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(10));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(1, 2, 3), list);
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(11));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(3), list);
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(9));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(4), list);
        }
        fieldKeeper.insert(7, 4);
        fieldKeeper.insert(6, 2);
        fieldKeeper.insert(8, 5);
        fieldKeeper.insert(7, 4);
        fieldKeeper.insert(6, 4);
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(7));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(4), list);
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(6));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(2, 4), list);
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(8));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(5), list);
        }
        fieldKeeper.insert(4, 1);
        fieldKeeper.insert(5, 2);
        fieldKeeper.insert(3, 3);
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(4));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(1), list);
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(5));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(2), list);
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(3));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(3), list);
        }
        fieldKeeper.insert(0, 14);
        fieldKeeper.insert(-5, 8);
        fieldKeeper.insert(2, 13);
        fieldKeeper.insert(10, 100);
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(0));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(14), list);
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(-5));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(8), list);
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(2));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(13), list);
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(10));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(1, 2, 3, 100), list);
        }
        fieldKeeper.insert(null, 65);
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search((Integer) null));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(65, 121), list);
        }
    }

    @Test
    public void searchTest() {
        {
            final FieldKeeper<Integer, Integer> fieldKeeper = prepareFieldKeeper(Integer.class, "int");
            assertEquals(Collections.emptySet(), fieldKeeper.search(10));
            for (int i = 15; i < 35; i++) {
                fieldKeeper.insert(i, i);
            }
            fieldKeeper.insert(10, 1);
            fieldKeeper.insert(8, 2);
            fieldKeeper.insert(9, 3);
            fieldKeeper.insert(6, 4);
            fieldKeeper.insert(7, 5);
            fieldKeeper.insert(12, 6);
            fieldKeeper.insert(11, 7);
            fieldKeeper.insert(14, 8);
            fieldKeeper.insert(13, 9);
            fieldKeeper.insert(null, 18);
            {
                final List<Integer> list = new ArrayList(fieldKeeper.conditionSearch(new SimpleCondition(ICondition.SimpleType.EQ, "int", 6)));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(4), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.conditionSearch(new SimpleCondition(ICondition.SimpleType.NOT, "int", 6)));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 2, 3, 5, 6, 7, 8, 9, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.conditionSearch(new SimpleCondition(ICondition.SimpleType.GT, "int", 9)));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 6, 7, 8, 9, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.conditionSearch(new SimpleCondition(ICondition.SimpleType.GTE, "int", 9)));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 3, 6, 7, 8, 9, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.conditionSearch(new SimpleCondition(ICondition.SimpleType.LT, "int", 10)));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(2, 3, 4, 5), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.conditionSearch(new SimpleCondition(ICondition.SimpleType.LTE, "int", 10)));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 2, 3, 4, 5), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.conditionSearch(new SimpleCondition(ICondition.SimpleType.EQ, "int", null)));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(18), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.conditionSearch(new SimpleCondition(ICondition.SimpleType.NOT, "int", null)));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34), list);
            }
        }
        {
            final FieldKeeper<String, Integer> fieldKeeper = prepareFieldKeeper(String.class, "String");
            assertEquals(Collections.emptySet(), fieldKeeper.search("test10"));
            fieldKeeper.insert("test10", 1);
            fieldKeeper.insert("test8", 2);
            fieldKeeper.insert("test9", 3);
            fieldKeeper.insert("test6", 4);
            fieldKeeper.insert("test7", 5);
            fieldKeeper.insert("test12", 6);
            fieldKeeper.insert("test11", 7);
            fieldKeeper.insert("test14", 8);
            fieldKeeper.insert("test13", 9);
            {
                final List<Integer> list = new ArrayList(fieldKeeper.conditionSearch(new SimpleCondition(ICondition.SimpleType.EQ, "String", "test6")));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(4), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.conditionSearch(new SimpleCondition(ICondition.SimpleType.NOT, "String", "test6")));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 2, 3, 5, 6, 7, 8, 9), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.conditionSearch(new SimpleCondition(ICondition.SimpleType.LT, "String", "test9")));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 2, 4, 5, 6, 7, 8, 9), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.conditionSearch(new SimpleCondition(ICondition.SimpleType.LTE, "String", "test9")));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.conditionSearch(new SimpleCondition(ICondition.SimpleType.GT, "String", "test10")));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(2, 3, 4, 5, 6, 7, 8, 9), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.conditionSearch(new SimpleCondition(ICondition.SimpleType.GTE, "String", "test10")));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.conditionSearch(new SimpleCondition(ICondition.SimpleType.LIKE, "String", "test1")));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 6, 7, 8, 9), list);
            }
        }
    }

    @Test
    public void deleteTest() {
        final FieldKeeper<Integer, Integer> fieldKeeper = prepareFieldKeeper(Integer.class, "int");
        assertEquals(Collections.emptySet(), fieldKeeper.search(10));
        fieldKeeper.insert(10, 1);
        fieldKeeper.insert(10, 2);
        fieldKeeper.insert(null, 65);
        fieldKeeper.insert(null, 66);
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(10));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(1, 2), list);
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search((Integer) null));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(65, 66), list);
        }
        fieldKeeper.insert(11, 3);
        fieldKeeper.insert(10, 3);
        fieldKeeper.insert(9, 4);
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(10));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(1, 2, 3), list);
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(11));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(3), list);
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(9));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(4), list);
        }
        fieldKeeper.insert(8, 5);
        fieldKeeper.insert(12, 6);
        fieldKeeper.insert(7, 7);
        fieldKeeper.insert(13, 8);
        fieldKeeper.insert(14, 2);
        fieldKeeper.insert(1, 9);
        fieldKeeper.insert(15, 44);
        fieldKeeper.insert(-8, 29);
        {
            final FieldKeeper.DeleteResult deleteResult = fieldKeeper.delete(9, 4);
            assertTrue(deleteResult.deleted);
            assertTrue(deleteResult.fully);
        }
        {
            final FieldKeeper.DeleteResult deleteResult = fieldKeeper.delete(11, 3);
            assertTrue(deleteResult.deleted);
            assertTrue(deleteResult.fully);
        }
        {
            final FieldKeeper.DeleteResult deleteResult = fieldKeeper.delete(10, 2);
            assertTrue(deleteResult.deleted);
            assertFalse(deleteResult.fully);
        }
        {
            final FieldKeeper.DeleteResult deleteResult = fieldKeeper.delete(null, 65);
            assertTrue(deleteResult.deleted);
            assertFalse(deleteResult.fully);
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(10));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(1, 3), list);
        }
        {
            assertEquals(Collections.emptySet(), fieldKeeper.search(11));
        }
        {
            assertEquals(Collections.emptySet(), fieldKeeper.search(9));
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(8));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(5), list);
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(12));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(6), list);
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(7));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(7), list);
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(13));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(8), list);
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search((Integer) null));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(66), list);
        }
        {
            final FieldKeeper.DeleteResult deleteResult = fieldKeeper.delete(8, 4);
            assertFalse(deleteResult.deleted);
            assertFalse(deleteResult.fully);
        }
        {
            final FieldKeeper.DeleteResult deleteResult = fieldKeeper.delete(12, 6);
            assertTrue(deleteResult.deleted);
            assertTrue(deleteResult.fully);
        }
        {
            final FieldKeeper.DeleteResult deleteResult = fieldKeeper.delete(1, 9);
            assertTrue(deleteResult.deleted);
            assertTrue(deleteResult.fully);
        }
        {
            final FieldKeeper.DeleteResult deleteResult = fieldKeeper.delete(7, 7);
            assertTrue(deleteResult.deleted);
            assertTrue(deleteResult.fully);
        }
        {
            final FieldKeeper.DeleteResult deleteResult = fieldKeeper.delete(13, 9);
            assertFalse(deleteResult.deleted);
            assertFalse(deleteResult.fully);
        }
        {
            final FieldKeeper.DeleteResult deleteResult = fieldKeeper.delete(null, 66);
            assertTrue(deleteResult.deleted);
            assertTrue(deleteResult.fully);
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(10));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(1, 3), list);
        }
        {
            assertEquals(Collections.emptySet(), fieldKeeper.search(11));
        }
        {
            assertEquals(Collections.emptySet(), fieldKeeper.search(9));
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(8));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(5), list);
        }
        {
            assertEquals(Collections.emptySet(), fieldKeeper.search(12));
        }
        {
            assertEquals(Collections.emptySet(), fieldKeeper.search(7));
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(13));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(8), list);
        }
        {
            assertEquals(Collections.emptySet(), fieldKeeper.search((Integer) null));
        }
    }

    @Test
    public void transformTest() {
        final FieldKeeper<Integer, Integer> fieldKeeper = prepareFieldKeeper(Integer.class, "int");
        fieldKeeper.insert(10, 1);
        fieldKeeper.insert(8, 2);
        fieldKeeper.insert(9, 3);
        fieldKeeper.insert(6, 4);
        fieldKeeper.insert(7, 5);
        fieldKeeper.insert(12, 6);
        fieldKeeper.insert(11, 7);
        fieldKeeper.insert(14, 8);
        fieldKeeper.insert(13, 9);
        fieldKeeper.insert(null, 15);
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search((Integer) null));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(15), list);
        }
        fieldKeeper.transform(6, 7, 7);
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(6));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(4), list);
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(7));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(5), list);
        }
        fieldKeeper.transform(6, 7, 4);
        {
            assertEquals(Collections.emptySet(), fieldKeeper.search(6));
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(7));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(4, 5), list);
        }
        fieldKeeper.transform(12, 11, 6);
        {
            assertEquals(Collections.emptySet(), fieldKeeper.search(12));
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(11));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(6, 7), list);
        }
        fieldKeeper.transform(null, 21, 15);
        {
            assertEquals(Collections.emptySet(), fieldKeeper.search((Integer) null));
        }
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(21));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(15), list);
        }
    }

    @Test
    public void clearTest() {
        final String fileName = "test.int";
        assertFalse(new File(fileName).exists());
        {
            final FieldKeeper<Integer, Integer> fieldKeeper = prepareFieldKeeper(Integer.class, "int");
            fieldKeeper.insert(1, 1);
            assertEquals(Collections.singletonList(1), new ArrayList<>(fieldKeeper.search(1)));
            fieldKeeper.destroy();
        }
        assertTrue(new File(fileName).exists());
        {
            final FieldKeeper<Integer, Integer> fieldKeeper = prepareFieldKeeper(Integer.class, "int");
            assertEquals(Collections.singletonList(1), new ArrayList<>(fieldKeeper.search(1)));
            fieldKeeper.clear();
        }
        for (File file : Objects.requireNonNull(new File(System.getProperty("user.dir")).listFiles((file, name) -> name.endsWith("int")))) {
            assertFalse(file.exists());
        }
    }

    @Test
    public void concurrentTest() {
        final FieldKeeper<Integer, Integer> fieldKeeper = prepareFieldKeeper(Integer.class, "int");
        final int max = fieldKeeper instanceof BPlusTree ? ((BPlusTree<Integer, Integer>) fieldKeeper).treeFactor * 20 : 100;
        try {
            final AtomicInteger count = new AtomicInteger();
            final Thread thread1 = new Thread(() -> {
                for (int i = 0; i < max; i++) {
                    fieldKeeper.insert(i, i);
                }
                System.out.println(Thread.currentThread().getName() + " finished");
            });
            final Thread thread2 = new Thread(() -> {
                for (int i = 0; i < 100 + max; i++) {
                    if (i % 2 == 0) {
                        fieldKeeper.delete(i, i);
                    }
                }
                System.out.println(Thread.currentThread().getName() + " finished");
            });
            final Thread thread3 = new Thread(() -> {
                for (int i = 0; i < max; i++) {
                    if (i % 2 != 0) {
                        fieldKeeper.transform(i, i + 1, i);
                    }
                }
                System.out.println(Thread.currentThread().getName() + " finished");
            });
            final Thread thread4 = new Thread(() -> {
                for (int i = 0; i < max; i++) {
                    if (!fieldKeeper.search(i).isEmpty()) {
                        count.incrementAndGet();
                    }
                }
                System.out.println(Thread.currentThread().getName() + " finished");
            });
            thread1.start();
            thread2.start();
            thread3.start();
            thread4.start();
            try {
                thread1.join();
                thread2.join();
                thread3.join();
                thread4.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("count " + count.get());
            System.out.println();
            count.set(0);
            for (int i = 0; i < max; i++) {
                if (!fieldKeeper.search(i).isEmpty()) {
                    count.incrementAndGet();
                }
            }
            System.out.println("count " + count.get());
            System.out.println();
            System.out.println("main thread finished");
        } finally {
            fieldKeeper.clear();
        }
    }

    abstract <T extends Comparable<T>> FieldKeeper<T, Integer> prepareFieldKeeper(Class<T> clazz, String fieldName);

}
