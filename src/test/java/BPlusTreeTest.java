import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import server.model.FieldKeeper;
import server.model.ObjectConverter;
import server.model.impl.BPlusTree;
import server.model.impl.ObjectConverterImpl;
import server.model.pojo.Pair;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@RunWith(Parameterized.class)
public class BPlusTreeTest extends FieldKeeperTest {
    protected final int treeFactor;

    public BPlusTreeTest(int treeFactor) {
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
        return new TestBPlusTree<>(fieldName, "test", new ObjectConverterImpl(), treeFactor);
    }

    private static class TestBPlusTree<U extends Comparable<U>, V> extends BPlusTree<U, V> {
        public TestBPlusTree(String fieldName, String path, ObjectConverter objectConverter, int treeFactor) {
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
                assert isLeaf(node) || ((InternalNode<U, V>) node).children.size() == 0;
                return;
            }
            assert node == getVariables().root || node.pairs.size() >= this.treeFactor - 1;
            assert node.pairs.size() <= 2 * this.treeFactor - 1;
            assert isLeaf(node) || node == getVariables().root || ((InternalNode<U, V>) node).children.size() == node.pairs.size() + 1;
            Pair<U, Set<V>> previous = checkPair(node, 0);
            for (int i = 1; i < node.pairs.size(); i++) {
                final Pair<U, Set<V>> pair = checkPair(node, i);
                assert previous.getFirst().compareTo(pair.getFirst()) < 0;
            }
        }

        private Pair<U, Set<V>> checkPair(Node<U, V> node, int index) {
            Pair<U, Set<V>> pair = node.pairs.get(index);
            if (isLeaf(node) || ((InternalNode<U, V>) node).children.size() == 0) {
                return pair;
            }

            Node<U, V> childLeft = ((InternalNode<U, V>) node).children.get(index);
            assert !isLeaf(childLeft) || !isInitialized(node) && new File(((LeafNode<U, V>) childLeft).fileName).exists();
            childLeft = readChild(node, index);
            assert childLeft.pairs.get(childLeft.pairs.size() - 1).getFirst().compareTo(pair.getFirst()) < 0;
            checkNode(childLeft);

            Node<U, V> childRight = ((InternalNode<U, V>) node).children.get(index + 1);
            assert !isLeaf(childRight) || !isInitialized(node) && new File(((LeafNode<U, V>) childRight).fileName).exists();
            childRight = readChild(node, index + 1);
            assert childRight.pairs.get(0).getFirst().compareTo(pair.getFirst()) > 0;
            checkNode(childRight);

            return pair;
        }
    }

}
