import sample.model.ConditionServiceImpl;
import sample.model.FieldKeeper;
import sample.model.FieldMap;

import java.util.concurrent.ConcurrentHashMap;

public class FieldMapTest extends FieldKeeperTest {

    @Override
    protected <T extends Comparable> FieldKeeper<T, Integer> prepareFieldKeeper(Class<T> clazz, String fieldName) {
        return new FieldMap<>(fieldName, new ConditionServiceImpl(TestUtils.mockModelService()), new ConcurrentHashMap<>());
    }
}
