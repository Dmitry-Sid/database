import server.model.FieldKeeper;
import server.model.impl.ConditionServiceImpl;
import server.model.impl.DataCompressorImpl;
import server.model.impl.FieldMap;
import server.model.impl.ObjectConverterImpl;

public class FieldMapTest extends FieldKeeperTest {

    @Override
    protected <T extends Comparable<T>> FieldKeeper<T, Integer> prepareFieldKeeper(Class<T> clazz, String fieldName) {
        return new FieldMap<>(fieldName, "test", new ObjectConverterImpl(new DataCompressorImpl()), new ConditionServiceImpl(TestUtils.mockModelService()));
    }
}
