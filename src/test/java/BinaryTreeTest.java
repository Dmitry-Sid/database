import server.model.impl.BinaryTree;
import server.model.impl.ConditionServiceImpl;
import server.model.FieldKeeper;

public class BinaryTreeTest extends FieldKeeperTest {

    @Override
    protected <T extends Comparable> FieldKeeper<T, Integer> prepareFieldKeeper(Class<T> clazz, String fieldName) {
        return new BinaryTree<>(fieldName, new ConditionServiceImpl(TestUtils.mockModelService()));
    }
}
