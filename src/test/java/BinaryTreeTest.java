import server.model.FieldKeeper;
import server.model.impl.BinaryTree;
import server.model.impl.ConditionServiceImpl;
import server.model.impl.DataCompressorImpl;
import server.model.impl.ObjectConverterImpl;

public class BinaryTreeTest extends FieldKeeperTest {

    @Override
    protected <T extends Comparable<T>> FieldKeeper<T, Integer> prepareFieldKeeper(Class<T> clazz, String fieldName) {
        return new BinaryTree<>(fieldName, "test", new ObjectConverterImpl(new DataCompressorImpl()), new ConditionServiceImpl(TestUtils.mockModelService()));
    }
}
