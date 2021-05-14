import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import server.model.FieldKeeper;
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
        return new BTree<>(fieldName, "test", new ObjectConverterImpl(), treeFactor);
    }
}
