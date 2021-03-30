import org.junit.Test;
import server.model.FieldKeeper;
import server.model.impl.ConditionServiceImpl;
import server.model.pojo.ICondition;
import server.model.pojo.SimpleCondition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public abstract class FieldKeeperTest {

    @Test
    public void insertTest() {
        final FieldKeeper<Integer, Integer> fieldKeeper = prepareFieldKeeper(Integer.class, "int");
        assertEquals(Collections.emptySet(), fieldKeeper.search(10));
        fieldKeeper.insert(10, 1);
        fieldKeeper.insert(10, 2);
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(10));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(1, 2), list);
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
    }

    @Test
    public void searchTest() {
        {
            final FieldKeeper<Integer, Integer> fieldKeeper = prepareFieldKeeper(Integer.class, "int");
            assertEquals(Collections.emptySet(), fieldKeeper.search(10));
            fieldKeeper.insert(10, 1);
            fieldKeeper.insert(8, 2);
            fieldKeeper.insert(9, 3);
            fieldKeeper.insert(6, 4);
            fieldKeeper.insert(7, 5);
            fieldKeeper.insert(12, 6);
            fieldKeeper.insert(11, 7);
            fieldKeeper.insert(14, 8);
            fieldKeeper.insert(13, 9);
            {
                final List<Integer> list = new ArrayList(fieldKeeper.search(new ConditionServiceImpl(TestUtils.mockModelService()), new SimpleCondition(ICondition.SimpleType.EQ, "int", 6)));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(4), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.search(new ConditionServiceImpl(TestUtils.mockModelService()), new SimpleCondition(ICondition.SimpleType.NOT, "int", 6)));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 2, 3, 5, 6, 7, 8, 9), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.search(new ConditionServiceImpl(TestUtils.mockModelService()), new SimpleCondition(ICondition.SimpleType.GT, "int", 9)));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 6, 7, 8, 9), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.search(new ConditionServiceImpl(TestUtils.mockModelService()), new SimpleCondition(ICondition.SimpleType.GTE, "int", 9)));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 3, 6, 7, 8, 9), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.search(new ConditionServiceImpl(TestUtils.mockModelService()), new SimpleCondition(ICondition.SimpleType.LT, "int", 10)));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(2, 3, 4, 5), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.search(new ConditionServiceImpl(TestUtils.mockModelService()), new SimpleCondition(ICondition.SimpleType.LTE, "int", 10)));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 2, 3, 4, 5), list);
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
                final List<Integer> list = new ArrayList(fieldKeeper.search(new ConditionServiceImpl(TestUtils.mockModelService()), new SimpleCondition(ICondition.SimpleType.EQ, "String", "test6")));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(4), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.search(new ConditionServiceImpl(TestUtils.mockModelService()), new SimpleCondition(ICondition.SimpleType.NOT, "String", "test6")));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 2, 3, 5, 6, 7, 8, 9), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.search(new ConditionServiceImpl(TestUtils.mockModelService()), new SimpleCondition(ICondition.SimpleType.LT, "String", "test9")));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 2, 4, 5, 6, 7, 8, 9), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.search(new ConditionServiceImpl(TestUtils.mockModelService()), new SimpleCondition(ICondition.SimpleType.LTE, "String", "test9")));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.search(new ConditionServiceImpl(TestUtils.mockModelService()), new SimpleCondition(ICondition.SimpleType.GT, "String", "test10")));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(2, 3, 4, 5, 6, 7, 8, 9), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.search(new ConditionServiceImpl(TestUtils.mockModelService()), new SimpleCondition(ICondition.SimpleType.GTE, "String", "test10")));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9), list);
            }
            {
                final List<Integer> list = new ArrayList(fieldKeeper.search(new ConditionServiceImpl(TestUtils.mockModelService()), new SimpleCondition(ICondition.SimpleType.LIKE, "String", "test1")));
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
        {
            final List<Integer> list = new ArrayList(fieldKeeper.search(10));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(1, 2), list);
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

        assertTrue(fieldKeeper.delete(9, 4));
        assertTrue(fieldKeeper.delete(11, 3));
        assertTrue(fieldKeeper.delete(10, 2));
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
        assertFalse(fieldKeeper.delete(8, 4));
        assertTrue(fieldKeeper.delete(12, 6));
        assertTrue(fieldKeeper.delete(7, 7));
        assertFalse(fieldKeeper.delete(13, 9));
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
    }

    protected abstract <T extends Comparable> FieldKeeper<T, Integer> prepareFieldKeeper(Class<T> clazz, String fieldName);

}
