import org.junit.Before;
import org.junit.Test;
import server.model.ConditionException;
import server.model.FieldKeeper;
import server.model.IndexService;
import server.model.impl.ConditionServiceImpl;
import server.model.impl.IndexServiceImpl;
import server.model.pojo.EmptyCondition;
import server.model.pojo.ICondition;
import server.model.pojo.MultiComplexCondition;
import server.model.pojo.SimpleCondition;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class IndexServiceTest {

    private Boolean[] transformed;
    private Boolean[] inserted;
    private Boolean[] deleted;
    private final IndexService indexService = prepareIndexService();

    public IndexServiceTest() throws ConditionException {
    }

    @Before
    public void before() {
        transformed = new Boolean[]{false, false};
        inserted = new Boolean[]{false, false};
        deleted = new Boolean[]{false, false};
    }

    @Test
    public void searchTest() throws ConditionException {
        {
            final IndexService.SearchResult searchResult = indexService.search(null, -1);
            assertFalse(searchResult.found);
        }
        {
            final IndexService.SearchResult searchResult = indexService.search(new EmptyCondition(), -1);
            assertFalse(searchResult.found);
        }
        {
            final ICondition condition = SimpleCondition.make(ICondition.SimpleType.GT, "int", 20);
            final IndexService.SearchResult searchResult = indexService.search(condition, -1);
            assertTrue(searchResult.found);
            assertEquals(51, searchResult.idSet.size());
            final AtomicInteger i = new AtomicInteger(10);
            searchResult.idSet.stream().sorted().forEach(id -> {
                assertEquals((Integer) i.get(), id);
                i.getAndIncrement();
            });
        }
        {
            final ICondition condition = SimpleCondition.make(ICondition.SimpleType.GT, "int", 20);
            final IndexService.SearchResult searchResult = indexService.search(condition, 10);
            assertTrue(searchResult.found);
            assertEquals(10, searchResult.idSet.size());
            final AtomicInteger i = new AtomicInteger(10);
            searchResult.idSet.stream().sorted().forEach(id -> {
                assertEquals((Integer) i.get(), id);
                i.getAndIncrement();
            });
        }
        {
            final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LT, "int", 20);
            final IndexService.SearchResult searchResult = indexService.search(condition, -1);
            assertTrue(searchResult.found);
            assertEquals(51, searchResult.idSet.size());
            final AtomicInteger i = new AtomicInteger(70);
            searchResult.idSet.stream().sorted().forEach(id -> {
                assertEquals((Integer) i.get(), id);
                i.getAndIncrement();
            });
        }
        {
            final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LT, "int", 20);
            final IndexService.SearchResult searchResult = indexService.search(condition, 30);
            assertTrue(searchResult.found);
            assertEquals(30, searchResult.idSet.size());
            final AtomicInteger i = new AtomicInteger(70);
            searchResult.idSet.stream().sorted().forEach(id -> {
                assertEquals((Integer) i.get(), id);
                i.getAndIncrement();
            });
        }
        {
            final ICondition condition = SimpleCondition.make(ICondition.SimpleType.GT, "String", "se");
            final IndexService.SearchResult searchResult = indexService.search(condition, -1);
            assertTrue(searchResult.found);
            assertEquals(51, searchResult.idSet.size());
            final AtomicInteger i = new AtomicInteger(20);
            searchResult.idSet.stream().sorted().forEach(id -> {
                assertEquals((Integer) i.get(), id);
                i.getAndIncrement();
            });
        }
        {
            final ICondition condition = SimpleCondition.make(ICondition.SimpleType.GT, "String", "se");
            final IndexService.SearchResult searchResult = indexService.search(condition, 29);
            assertTrue(searchResult.found);
            assertEquals(29, searchResult.idSet.size());
            final AtomicInteger i = new AtomicInteger(20);
            searchResult.idSet.stream().sorted().forEach(id -> {
                assertEquals((Integer) i.get(), id);
                i.getAndIncrement();
            });
        }
        {
            final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LT, "String", "te");
            final IndexService.SearchResult searchResult = indexService.search(condition, -1);
            assertTrue(searchResult.found);
            assertEquals(51, searchResult.idSet.size());
            final AtomicInteger i = new AtomicInteger(80);
            searchResult.idSet.stream().sorted().forEach(id -> {
                assertEquals((Integer) i.get(), id);
                i.getAndIncrement();
            });
        }
        {
            final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LT, "String", "te");
            final IndexService.SearchResult searchResult = indexService.search(condition, 18);
            assertTrue(searchResult.found);
            assertEquals(18, searchResult.idSet.size());
            final AtomicInteger i = new AtomicInteger(80);
            searchResult.idSet.stream().sorted().forEach(id -> {
                assertEquals((Integer) i.get(), id);
                i.getAndIncrement();
            });
        }
        {
            final ICondition condition = MultiComplexCondition.make(ICondition.ComplexType.OR,
                    SimpleCondition.make(ICondition.SimpleType.GT, "int", 20),
                    SimpleCondition.make(ICondition.SimpleType.GT, "String", "se"));
            final IndexService.SearchResult searchResult = indexService.search(condition, -1);
            assertTrue(searchResult.found);
            assertEquals(61, searchResult.idSet.size());
            final AtomicInteger i = new AtomicInteger(10);
            searchResult.idSet.stream().sorted().forEach(id -> {
                assertEquals((Integer) i.get(), id);
                i.getAndIncrement();
            });
        }
        {
            final ICondition condition = MultiComplexCondition.make(ICondition.ComplexType.OR,
                    SimpleCondition.make(ICondition.SimpleType.GT, "int", 20),
                    SimpleCondition.make(ICondition.SimpleType.GT, "String", "se"));
            final IndexService.SearchResult searchResult = indexService.search(condition, 44);
            assertTrue(searchResult.found);
            assertEquals(44, searchResult.idSet.size());
        }
        {
            final ICondition condition = MultiComplexCondition.make(ICondition.ComplexType.AND,
                    SimpleCondition.make(ICondition.SimpleType.GT, "int", 20),
                    SimpleCondition.make(ICondition.SimpleType.GT, "String", "se"));
            final IndexService.SearchResult searchResult = indexService.search(condition, -1);
            assertTrue(searchResult.found);
            assertEquals(41, searchResult.idSet.size());
            final AtomicInteger i = new AtomicInteger(20);
            searchResult.idSet.stream().sorted().forEach(id -> {
                assertEquals((Integer) i.get(), id);
                i.getAndIncrement();
            });
        }
        {
            final ICondition condition = MultiComplexCondition.make(ICondition.ComplexType.AND,
                    SimpleCondition.make(ICondition.SimpleType.GT, "int", 20),
                    SimpleCondition.make(ICondition.SimpleType.GT, "String", "se"));
            final IndexService.SearchResult searchResult = indexService.search(condition, 25);
            assertTrue(searchResult.found);
            assertEquals(25, searchResult.idSet.size());
            final AtomicInteger i = new AtomicInteger(20);
            searchResult.idSet.stream().sorted().forEach(id -> {
                assertEquals((Integer) i.get(), id);
                i.getAndIncrement();
            });
        }
        {
            final ICondition condition = MultiComplexCondition.make(ICondition.ComplexType.OR,
                    SimpleCondition.make(ICondition.SimpleType.LT, "int", 20),
                    SimpleCondition.make(ICondition.SimpleType.GT, "String", "se"));
            final IndexService.SearchResult searchResult = indexService.search(condition, -1);
            assertTrue(searchResult.found);
            assertEquals(101, searchResult.idSet.size());
            final AtomicInteger i = new AtomicInteger(20);
            searchResult.idSet.stream().sorted().forEach(id -> {
                assertEquals((Integer) i.get(), id);
                i.getAndIncrement();
            });
        }
        {
            final ICondition condition = MultiComplexCondition.make(ICondition.ComplexType.AND,
                    SimpleCondition.make(ICondition.SimpleType.LT, "int", 20),
                    SimpleCondition.make(ICondition.SimpleType.LT, "String", "te"));
            final IndexService.SearchResult searchResult = indexService.search(condition, -1);
            assertTrue(searchResult.found);
            assertEquals(41, searchResult.idSet.size());
            final AtomicInteger i = new AtomicInteger(80);
            searchResult.idSet.stream().sorted().forEach(id -> {
                assertEquals((Integer) i.get(), id);
                i.getAndIncrement();
            });
        }
        {
            final ICondition condition = MultiComplexCondition.make(ICondition.ComplexType.AND,
                    SimpleCondition.make(ICondition.SimpleType.LT, "int", 19),
                    SimpleCondition.make(ICondition.SimpleType.LT, "String", "te"));
            final IndexService.SearchResult searchResult = indexService.search(condition, -1);
            assertTrue(searchResult.found);
            assertEquals(0, searchResult.idSet.size());
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
    public void insertIntTest() {
        assertFalse(inserted[0]);
        assertFalse(inserted[1]);
        indexService.insert(TestUtils.generateRow(1, 1), new HashSet<>(Collections.singletonList("int")));
        assertTrue(inserted[0]);
        assertFalse(inserted[1]);
    }

    @Test
    public void insertStringTest() {
        assertFalse(inserted[0]);
        assertFalse(inserted[1]);
        indexService.insert(TestUtils.generateRow(1, 1), new HashSet<>(Collections.singletonList("String")));
        assertFalse(inserted[0]);
        assertTrue(inserted[1]);
    }

    @Test
    public void insertBothTest() {
        assertFalse(inserted[0]);
        assertFalse(inserted[1]);
        indexService.insert(TestUtils.generateRow(1, 1), new HashSet<>(Arrays.asList("String", "int")));
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

    private IndexService prepareIndexService() throws ConditionException {
        final Map<String, FieldKeeper> fieldKeepers = new HashMap<>();
        fieldKeepers.put("int", mockIntFieldKeeper());
        fieldKeepers.put("String", mockStringFieldKeeper());
        return new IndexServiceImpl(fieldKeepers, new ConditionServiceImpl(TestUtils.mockModelService()));
    }

    private FieldKeeper<Integer, Integer> mockIntFieldKeeper() throws ConditionException {
        final FieldKeeper<Integer, Integer> fieldKeeper = (FieldKeeper<Integer, Integer>) mock(FieldKeeper.class);
        final ICondition condition1 = SimpleCondition.make(ICondition.SimpleType.GT, "int", 20);
        final ICondition condition2 = SimpleCondition.make(ICondition.SimpleType.LT, "int", 20);
        when(fieldKeeper.conditionSearch(any(SimpleCondition.class), anyInt())).thenAnswer(invocation -> {
            final ICondition condition = (ICondition) invocation.getArguments()[0];
            final int size = (int) invocation.getArguments()[1];
            final Set<Integer> set = new HashSet<>();
            int bound = size > -1 ? Math.min(50, size - 1) : 50;
            if (condition1.equals(condition)) {
                for (int i = 0; i <= bound; i++) {
                    set.add(i + 10);
                }
            } else if (condition2.equals(condition)) {
                for (int i = 0; i <= bound; i++) {
                    set.add(i + 70);
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

    private FieldKeeper<String, Integer> mockStringFieldKeeper() throws ConditionException {
        final FieldKeeper<String, Integer> fieldKeeper = (FieldKeeper<String, Integer>) mock(FieldKeeper.class);
        final ICondition condition1 = SimpleCondition.make(ICondition.SimpleType.GT, "String", "se");
        final ICondition condition2 = SimpleCondition.make(ICondition.SimpleType.LT, "String", "te");
        when(fieldKeeper.conditionSearch(any(SimpleCondition.class), anyInt())).thenAnswer(invocation -> {
            final ICondition condition = (ICondition) invocation.getArguments()[0];
            final int size = (int) invocation.getArguments()[1];
            final Set<Integer> set = new HashSet<>();
            int bound = size > -1 ? Math.min(50, size - 1) : 50;
            if (condition1.equals(condition)) {
                for (int i = 0; i <= bound; i++) {
                    set.add(i + 20);
                }
            } else if (condition2.equals(condition)) {
                for (int i = 0; i <= bound; i++) {
                    set.add(i + 80);
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
