import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import server.model.FieldKeeper;
import server.model.ObjectConverter;
import server.model.impl.BTree;
import server.model.impl.ObjectConverterImpl;
import server.model.pojo.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@RunWith(Parameterized.class)
public class BTreeTest extends FieldKeeperTest {
    protected final int treeFactor;

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

    private static class TestBTree<U extends Comparable<U>, V> extends BTree<U, V> {
        public TestBTree(String fieldName, String path, ObjectConverter objectConverter, int treeFactor) {
            super(fieldName, path, objectConverter, treeFactor);
        }

        @Override
        public void insert(U key, V value) {
            super.insert(key, value);
            checkTree();
        }

        @Override
        public DeleteResult delete(U key, V value) {
            final DeleteResult deleteResult = super.delete(key, value);
            checkTree();
            return deleteResult;
        }

        @Override
        public void transform(U oldKey, U key, V value) {
            super.transform(oldKey, key, value);
            checkTree();
        }

        private void checkTree() {
            checkNode(getVariables().root);
        }

        private void checkNode(Node<U, V> node) {
            if (node.pairs.size() == 0) {
                assert node == getVariables().root;
                assert node.children.size() == 0;
                return;
            }
            assert node == getVariables().root || node.pairs.size() >= this.treeFactor - 1;
            assert node.pairs.size() <= 2 * this.treeFactor - 1;
            if (node.leaf) {
                assert node.children.size() == 0;
            } else {
                assert node == getVariables().root || node.children.size() == node.pairs.size() + 1;
            }
            Pair<U, Set<V>> previous = checkPair(node, 0);
            for (int i = 1; i < node.pairs.size(); i++) {
                final Pair<U, Set<V>> pair = checkPair(node, i);
                assert previous.getFirst().compareTo(pair.getFirst()) < 0;
            }
        }

        private Pair<U, Set<V>> checkPair(Node<U, V> node, int index) {
            Pair<U, Set<V>> pair = node.pairs.get(index);
            if (node.children.size() == 0) {
                return pair;
            }

            final Node<U, V> childLeft = read(node.children.get(index));
            assert childLeft.pairs.get(childLeft.pairs.size() - 1).getFirst().compareTo(pair.getFirst()) < 0;
            checkNode(childLeft);

            final Node<U, V> childRight = read(node.children.get(index + 1));
            assert childRight.pairs.get(0).getFirst().compareTo(pair.getFirst()) > 0;
            checkNode(childRight);

            return pair;
        }
    }

}
