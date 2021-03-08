import sample.model.BinaryTree;
import sample.model.ConditionServiceImpl;
import sample.model.FieldKeeper;
import sample.model.ModelServiceImpl;

public class BinaryTreeTest extends FieldKeeperTest {

    @Override
    protected <T extends Comparable> FieldKeeper<T, Integer> prepareFieldKeeper(Class<T> clazz, String fieldName) {
        return new BinaryTree<>(fieldName, new ConditionServiceImpl(new ModelServiceImpl()));
    }
}
