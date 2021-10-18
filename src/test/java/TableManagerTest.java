import org.junit.After;
import org.junit.Test;
import org.mockito.Matchers;
import server.model.ModelService;
import server.model.RowRepository;
import server.model.TableManager;
import server.model.TableServiceFactory;
import server.model.impl.DataCompressorImpl;
import server.model.impl.DestroyServiceImpl;
import server.model.impl.ObjectConverterImpl;
import server.model.impl.TableManagerImpl;
import server.model.pojo.ICondition;
import server.model.pojo.Row;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class TableManagerTest {
    private static final String FILE_NAME = "tables";

    @After
    public void after() {
        new File(FILE_NAME).delete();
    }

    @Test
    public void fullTest() {
        {
            final TableManager tableManager = createTableManager();
            TestUtils.doAndSleep(tableManager, () -> {
                assertEquals(0, tableManager.getTables().size());
                assertNull(tableManager.getServiceHolder("test"));

                tableManager.create("test");

                assertEquals(1, tableManager.getTables().size());
                assertEquals("test", ((TestModelService) tableManager.getServiceHolder("test").modelService).tableName);
                assertEquals("test", ((TestRowRepository) tableManager.getServiceHolder("test").rowRepository).tableName);

                tableManager.create("test2");

                assertEquals(2, tableManager.getTables().size());
                assertEquals("test", ((TestModelService) tableManager.getServiceHolder("test").modelService).tableName);
                assertEquals("test", ((TestRowRepository) tableManager.getServiceHolder("test").rowRepository).tableName);
                assertEquals("test2", ((TestModelService) tableManager.getServiceHolder("test2").modelService).tableName);
                assertEquals("test2", ((TestRowRepository) tableManager.getServiceHolder("test2").rowRepository).tableName);

                tableManager.delete("test");

                assertEquals(1, tableManager.getTables().size());
                assertNull(tableManager.getServiceHolder("test"));
                assertEquals("test2", ((TestModelService) tableManager.getServiceHolder("test2").modelService).tableName);
                assertEquals("test2", ((TestRowRepository) tableManager.getServiceHolder("test2").rowRepository).tableName);
            });
        }
        {
            final TableManager tableManager = createTableManager();
            TestUtils.doAndSleep(tableManager, () -> {
                assertEquals(1, tableManager.getTables().size());
                assertNull(tableManager.getServiceHolder("test"));
                assertEquals("test2", ((TestModelService) tableManager.getServiceHolder("test2").modelService).tableName);
                assertEquals("test2", ((TestRowRepository) tableManager.getServiceHolder("test2").rowRepository).tableName);
            });
        }
    }

    private TableManager createTableManager() {
        return new TableManagerImpl("", false, new ObjectConverterImpl(new DataCompressorImpl()), new DestroyServiceImpl(1000), mockTableServiceFactory());
    }

    private TableServiceFactory mockTableServiceFactory() {
        final TableServiceFactory tableServiceFactory = mock(TableServiceFactory.class);
        doAnswer(invocation -> {
            final String table = (String) invocation.getArguments()[0];
            final Consumer<Map<Class<?>, Object>> consumer = (Consumer<Map<Class<?>, Object>>) invocation.getArguments()[1];
            final Map<Class<?>, Object> map = new HashMap<>();
            map.put(ModelService.class, new TestModelService(table));
            map.put(RowRepository.class, new TestRowRepository(table));
            consumer.accept(map);
            return null;
        }).when(tableServiceFactory).createServices(any(String.class), any(Consumer.class), Matchers.<Class<?>>anyVararg());
        return tableServiceFactory;
    }

    private static class TestModelService implements ModelService {
        private final String tableName;

        private TestModelService(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public boolean contains(String field) {
            return false;
        }

        @Override
        public Comparable getValue(String field, String value) {
            return null;
        }

        @Override
        public void add(String field, Class<?> type) {

        }

        @Override
        public void delete(String... fields) {

        }

        @Override
        public void addIndex(String... fields) {

        }

        @Override
        public void deleteIndex(String... fields) {

        }

        @Override
        public List<FieldInfo> getFields() {
            return null;
        }

        @Override
        public Set<String> getIndexedFields() {
            return null;
        }

        @Override
        public void subscribeOnFieldsChanges(Consumer<Set<String>> fieldsConsumer) {

        }

        @Override
        public void subscribeOnIndexesChanges(Consumer<Set<String>> fieldsConsumer) {

        }

        @Override
        public void destroy() {

        }
    }

    private static class TestRowRepository implements RowRepository {
        private final String tableName;

        private TestRowRepository(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public void add(Row row) {

        }

        @Override
        public int size(ICondition iCondition, int maxSize) {
            return 0;
        }

        @Override
        public List<Row> getList(ICondition iCondition, int from, int size) {
            return null;
        }

        @Override
        public void delete(int id) {

        }

        @Override
        public boolean process(int id, Consumer<Row> consumer) {
            return false;
        }

        @Override
        public void destroy() {

        }
    }

}
