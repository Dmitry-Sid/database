import server.model.FieldKeeper;
import server.model.impl.BinaryTree;
import server.model.impl.ObjectConverterImpl;

public class BinaryTreeTest extends FieldKeeperTest {

    @Override
    protected <T extends Comparable> FieldKeeper<T, Integer> prepareFieldKeeper(Class<T> clazz, String fieldName) {
        return new BinaryTree<>(fieldName, "test", new ObjectConverterImpl());
    }
}
