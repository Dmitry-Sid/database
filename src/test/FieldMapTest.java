import server.model.ConditionServiceImpl;
import server.model.FieldKeeper;
import server.model.FieldMap;

import java.util.concurrent.ConcurrentHashMap;

public class FieldMapTest extends FieldKeeperTest {

    @Override
    protected <T extends Comparable> FieldKeeper<T, Integer> prepareFieldKeeper(Class<T> clazz, String fieldName) {
        return new FieldMap<>(fieldName, new ConditionServiceImpl(TestUtils.mockModelService()), new ConcurrentHashMap<>());
    }
}
