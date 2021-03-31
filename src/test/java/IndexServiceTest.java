import org.junit.Test;
import server.model.ConditionService;
import server.model.FieldKeeper;
import server.model.IndexService;
import server.model.impl.ConditionServiceImpl;
import server.model.impl.IndexServiceImpl;
import server.model.pojo.ComplexCondition;
import server.model.pojo.EmptyCondition;
import server.model.pojo.ICondition;
import server.model.pojo.SimpleCondition;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class IndexServiceTest {

    private final Boolean[] transformed = new Boolean[]{false, false};
    private final Boolean[] inserted = new Boolean[]{false, false};
    private final Boolean[] deleted = new Boolean[]{false, false};
    private final IndexService indexService = prepareIndexService();

    @Test
    public void searchTest() {
        {
            final IndexService.SearchResult searchResult = indexService.search(null);
            assertFalse(searchResult.found);
        }
        {
            final IndexService.SearchResult searchResult = indexService.search(new EmptyCondition());
            assertFalse(searchResult.found);
        }
        {
            final ICondition condition = new SimpleCondition(ICondition.SimpleType.GT, "int", 20);
            final IndexService.SearchResult searchResult = indexService.search(condition);
            assertTrue(searchResult.found);
            assertEquals(51, searchResult.idSet.size());
            AtomicInteger i = new AtomicInteger(10);
            searchResult.idSet.stream().sorted().forEach(id -> {
                assertEquals((Integer) i.get(), id);
                i.getAndIncrement();
            });
        }
        {
            final ICondition condition = new SimpleCondition(ICondition.SimpleType.LT, "int", 20);
            final IndexService.SearchResult searchResult = indexService.search(condition);
            assertTrue(searchResult.found);
            assertEquals(51, searchResult.idSet.size());
            AtomicInteger i = new AtomicInteger(70);
            searchResult.idSet.stream().sorted().forEach(id -> {
                assertEquals((Integer) i.get(), id);
                i.getAndIncrement();
            });
        }
        {
            final ICondition condition = new SimpleCondition(ICondition.SimpleType.GT, "String", "se");
            final IndexService.SearchResult searchResult = indexService.search(condition);
            assertTrue(searchResult.found);
            assertEquals(51, searchResult.idSet.size());
            AtomicInteger i = new AtomicInteger(20);
            searchResult.idSet.stream().sorted().forEach(id -> {
                assertEquals((Integer) i.get(), id);
                i.getAndIncrement();
            });
        }
        {
            final ICondition condition = new SimpleCondition(ICondition.SimpleType.LT, "String", "te");
            final IndexService.SearchResult searchResult = indexService.search(condition);
            assertTrue(searchResult.found);
            assertEquals(51, searchResult.idSet.size());
            AtomicInteger i = new AtomicInteger(80);
            searchResult.idSet.stream().sorted().forEach(id -> {
                assertEquals((Integer) i.get(), id);
                i.getAndIncrement();
            });
        }
        {
            final ICondition condition = new ComplexCondition(ICondition.ComplexType.OR,
                    new SimpleCondition(ICondition.SimpleType.GT, "int", 20),
                    new SimpleCondition(ICondition.SimpleType.GT, "String", "se"));
            final IndexService.SearchResult searchResult = indexService.search(condition);
            assertTrue(searchResult.found);
            assertEquals(61, searchResult.idSet.size());
            AtomicInteger i = new AtomicInteger(10);
            searchResult.idSet.stream().sorted().forEach(id -> {
                assertEquals((Integer) i.get(), id);
                i.getAndIncrement();
            });
        }
        {
            final ICondition condition = new ComplexCondition(ICondition.ComplexType.AND,
                    new SimpleCondition(ICondition.SimpleType.GT, "int", 20),
                    new SimpleCondition(ICondition.SimpleType.GT, "String", "se"));
            final IndexService.SearchResult searchResult = indexService.search(condition);
            assertTrue(searchResult.found);
            assertEquals(41, searchResult.idSet.size());
            AtomicInteger i = new AtomicInteger(20);
            searchResult.idSet.stream().sorted().forEach(id -> {
                assertEquals((Integer) i.get(), id);
                i.getAndIncrement();
            });
        }
        {
            final ICondition condition = new ComplexCondition(ICondition.ComplexType.OR,
                    new SimpleCondition(ICondition.SimpleType.LT, "int", 20),
                    new SimpleCondition(ICondition.SimpleType.GT, "String", "se"));
            final IndexService.SearchResult searchResult = indexService.search(condition);
            assertTrue(searchResult.found);
            assertEquals(101, searchResult.idSet.size());
            AtomicInteger i = new AtomicInteger(20);
            searchResult.idSet.stream().sorted().forEach(id -> {
                assertEquals((Integer) i.get(), id);
                i.getAndIncrement();
            });
        }
        {
            final ICondition condition = new ComplexCondition(ICondition.ComplexType.AND,
                    new SimpleCondition(ICondition.SimpleType.LT, "int", 20),
                    new SimpleCondition(ICondition.SimpleType.LT, "String", "te"));
            final IndexService.SearchResult searchResult = indexService.search(condition);
            assertTrue(searchResult.found);
            assertEquals(41, searchResult.idSet.size());
            AtomicInteger i = new AtomicInteger(80);
            searchResult.idSet.stream().sorted().forEach(id -> {
                assertEquals((Integer) i.get(), id);
                i.getAndIncrement();
            });
        }
    }

    @Test
    public void transformTest() {
        assertFalse(transformed[0]);
        assertFalse(transformed[1]);
        indexService.transform(TestUtils.generateRow(1, 1), TestUtils.generateRow(1, 2));
        assertTrue(transformed[0]);
        assertTrue(transformed[1]);
    }

    @Test
    public void insertTest() {
        assertFalse(inserted[0]);
        assertFalse(inserted[1]);
        indexService.insert(TestUtils.generateRow(1, 1));
        assertTrue(inserted[0]);
        assertTrue(inserted[1]);
    }

    @Test
    public void deleteTest() {
        assertFalse(deleted[0]);
        assertFalse(deleted[1]);
        indexService.delete(TestUtils.generateRow(1, 1));
        assertTrue(deleted[0]);
        assertTrue(deleted[1]);
    }

    private IndexService prepareIndexService() {
        final Map<String, FieldKeeper> fieldKeepers = new HashMap<>();
        fieldKeepers.put("int", mockIntFieldKeeper());
        fieldKeepers.put("String", mockStringFieldKeeper());
        return new IndexServiceImpl(fieldKeepers, new ConditionServiceImpl(TestUtils.mockModelService()));
    }

    private FieldKeeper<Integer, Integer> mockIntFieldKeeper() {
        final FieldKeeper<Integer, Integer> fieldKeeper = (FieldKeeper<Integer, Integer>) mock(FieldKeeper.class);
        final ICondition condition1 = new SimpleCondition(ICondition.SimpleType.GT, "int", 20);
        final ICondition condition2 = new SimpleCondition(ICondition.SimpleType.LT, "int", 20);
        when(fieldKeeper.search(any(ConditionService.class), any(SimpleCondition.class))).thenAnswer(invocation -> {
            final ICondition condition = (ICondition) invocation.getArguments()[1];
            final Set<Integer> set = new LinkedHashSet<>();
            if (condition1.equals(condition)) {
                for (int i = 10; i <= 60; i++) {
                    set.add(i);
                }
            } else if (condition2.equals(condition)) {
                for (int i = 70; i <= 120; i++) {
                    set.add(i);
                }
            }
            return set;
        });
        when(fieldKeeper.getFieldName()).thenReturn("int");
        doAnswer(invocation -> {
            transformed[0] = true;
            return null;
        }).when(fieldKeeper).transform(any(Integer.class), any(Integer.class), any(Integer.class));
        doAnswer(invocation -> {
            inserted[0] = true;
            return null;
        }).when(fieldKeeper).insert(any(Integer.class), any(Integer.class));
        doAnswer(invocation -> {
            deleted[0] = true;
            return null;
        }).when(fieldKeeper).delete(any(Integer.class), any(Integer.class));
        return fieldKeeper;
    }

    private FieldKeeper<String, Integer> mockStringFieldKeeper() {
        final FieldKeeper<String, Integer> fieldKeeper = (FieldKeeper<String, Integer>) mock(FieldKeeper.class);
        final ICondition condition1 = new SimpleCondition(ICondition.SimpleType.GT, "String", "se");
        final ICondition condition2 = new SimpleCondition(ICondition.SimpleType.LT, "String", "te");
        when(fieldKeeper.search(any(ConditionService.class), any(SimpleCondition.class))).thenAnswer(invocation -> {
            final ICondition condition = (ICondition) invocation.getArguments()[1];
            final Set<Integer> set = new LinkedHashSet<>();
            if (condition1.equals(condition)) {
                for (int i = 20; i <= 70; i++) {
                    set.add(i);
                }
            } else if (condition2.equals(condition)) {
                for (int i = 80; i <= 130; i++) {
                    set.add(i);
                }
            }
            return set;
        });
        doAnswer(invocation -> {
            transformed[1] = true;
            return null;
        }).when(fieldKeeper).transform(any(String.class), any(String.class), any(Integer.class));
        doAnswer(invocation -> {
            inserted[1] = true;
            return null;
        }).when(fieldKeeper).insert(any(String.class), any(Integer.class));
        doAnswer(invocation -> {
            deleted[1] = true;
            return null;
        }).when(fieldKeeper).delete(any(String.class), any(Integer.class));
        when(fieldKeeper.getFieldName()).thenReturn("String");
        return fieldKeeper;
    }

}