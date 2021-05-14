import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import server.model.FieldKeeper;
import server.model.ObjectConverter;
import server.model.impl.BTree;
import server.model.impl.ObjectConverterImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class BTreeTest extends FieldKeeperTest {
    private final int treeFactor;

    public BTreeTest(int treeFactor) {
        this.treeFactor = treeFactor;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        final List<Object[]> data = new ArrayList<>();
        for (int i = 2; i <= 15; i++) {
            data.add(new Object[]{i});
        }
        return data;
    }

    @Override
    <T extends Comparable<T>> FieldKeeper<T, Integer> prepareFieldKeeper(Class<T> clazz, String fieldName) {
        return new TestBTree<>(fieldName, "test", new ObjectConverterImpl(), treeFactor);
    }

    private static class TestBTree<T extends Comparable<T>> extends BTree<T, Integer> {
        public TestBTree(String fieldName, String path, ObjectConverter objectConverter, int treeFactor) {
            super(fieldName, path, objectConverter, treeFactor);
        }

        @Override
        public void insert(T key, Integer value) {
            super.insert(key, value);
            checkTree();
        }

        @Override
        public DeleteResult delete(T key, Integer value) {
            final DeleteResult deleteResult = super.delete(key, value);
            checkTree();
            return deleteResult;
        }

        @Override
        public void transform(T oldKey, T key, Integer value) {
            super.transform(oldKey, key, value);
            checkTree();
        }
    }

}
