import sample.model.ConditionServiceImpl;
import sample.model.FieldKeeper;
import sample.model.FieldMap;
import sample.model.ModelServiceImpl;

import java.util.concurrent.ConcurrentHashMap;

public class FieldMapTest extends FieldKeeperTest {

    @Override
    protected <T extends Comparable> FieldKeeper<T, Integer> prepareFieldKeeper(Class<T> clazz, String fieldName) {
        return new FieldMap<>(fieldName, new ConditionServiceImpl(new ModelServiceImpl()), new ConcurrentHashMap<>());
    }
}
