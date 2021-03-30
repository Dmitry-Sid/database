import server.model.FieldKeeper;
import server.model.impl.FieldMap;

import java.util.concurrent.ConcurrentHashMap;

public class FieldMapTest extends FieldKeeperTest {

    @Override
    protected <T extends Comparable> FieldKeeper<T, Integer> prepareFieldKeeper(Class<T> clazz, String fieldName) {
        return new FieldMap<>(fieldName, new ConcurrentHashMap<>());
    }
}
