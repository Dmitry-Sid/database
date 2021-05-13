import server.model.FieldKeeper;
import server.model.impl.BTree;
import server.model.impl.ObjectConverterImpl;

import java.util.Random;

public class BTreeTest extends FieldKeeperTest {

    @Override
    <T extends Comparable<T>> FieldKeeper<T, Integer> prepareFieldKeeper(Class<T> clazz, String fieldName) {
        return new BTree<>(fieldName, "test", new ObjectConverterImpl(), new Random().nextInt(15 - 2) + 2);
    }
}
