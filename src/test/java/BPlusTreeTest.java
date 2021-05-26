import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import server.model.ConditionService;
import server.model.FieldKeeper;
import server.model.ObjectConverter;
import server.model.impl.BPlusTree;
import server.model.impl.ConditionServiceImpl;
import server.model.impl.ObjectConverterImpl;
import server.model.lock.LockService;
import server.model.pojo.Pair;

import java.util.*;
import java.util.function.Function;

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
        return new TestBPlusTree<>(fieldName, "test", new ObjectConverterImpl(), new ConditionServiceImpl(TestUtils.mockModelService()), treeFactor);
    }

    private static class TestBPlusTree<U extends Comparable<U>, V> extends BPlusTree<U, V> {
        private Node<U, V> root;
        private Map<String, LeafNode<U, V>> map;

        private TestBPlusTree(String fieldName, String path, ObjectConverter objectConverter, ConditionService conditionService, int treeFactor) {
            super(fieldName, path, objectConverter, conditionService, 10, treeFactor);
        }

        @Override
        public void insert(U key, V value) {
            LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Write, () -> {
                setStateBefore();
                super.insert(key, value);
                checkTree();
            });
        }

        @Override
        public DeleteResult delete(U key, V value) {
            return LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Write, () -> {
                setStateBefore();
                final DeleteResult deleteResult = super.delete(key, value);
                checkTree();
                return deleteResult;
            });
        }

        private void fillMap(InternalNode<U, V> node, Map<String, LeafNode<U, V>> map) {
            for (Node<U, V> child : getChildren(node)) {
                if (isLeaf(child)) {
                    final LeafNode<U, V> leafChild = (LeafNode<U, V>) child;
                    map.put(leafChild.fileName, objectConverter.fromFile(leafChild.getClass(), leafChild.fileName));
                } else {
                    fillMap((InternalNode<U, V>) child, map);
                }
            }
        }

        @Override
        public void transform(U oldKey, U key, V value) {
            LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Write, () -> {
                setStateBefore();
                super.transform(oldKey, key, value);
                checkTree();
            });
        }

        private void setStateBefore() {
            if (isLeaf(getVariables().root)) {
                root = new LeafNode<>(((LeafNode<U, V>) getVariables().root).fileName);
            } else {
                root = new InternalNode<>();
                getChildren(root).addAll(getChildren(getVariables().root));
                map = new HashMap<>();
                fillMap((InternalNode<U, V>) root, map);
            }
            root.pairs = getVariables().root.pairs;
        }

        private void checkTree() {
            checkNode(getVariables().root);
        }

        private void checkNode(Node<U, V> node) {
            if (node.pairs.size() == 0) {
                assert node == getVariables().root;
                assert isLeaf(node) || getChildren(node).size() == 0;
                return;
            }
            assert node == getVariables().root || node.pairs.size() >= this.treeFactor - 1;
            assert node.pairs.size() <= 2 * this.treeFactor - 1;

            assert isLeaf(node) || node == getVariables().root || getChildren(node).size() == node.pairs.size() + 1;
            Pair<U, Set<V>> previous = checkPair(node, 0);
            for (int i = 1; i < node.pairs.size(); i++) {
                final Pair<U, Set<V>> pair = checkPair(node, i);
                assert previous.getFirst().compareTo(pair.getFirst()) < 0;
            }
        }

        private Pair<U, Set<V>> checkPair(Node<U, V> node, int index) {
            Pair<U, Set<V>> pair = node.pairs.get(index);
            if (isLeaf(node) || getChildren(node).size() == 0) {
                return pair;
            }

            Node<U, V> childLeft = readChild(node, index);
            checkChildPair(pair, childLeft, result -> result < 0);
            checkNode(childLeft);

            Node<U, V> childRight = readChild(node, index + 1);
            checkChildPair(pair, childRight, result -> result > 0);
            checkNode(childRight);

            return pair;
        }

        private void checkChildPair(Pair<U, Set<V>> pair, Node<U, V> child, Function<Integer, Boolean> compareFunction) {
            for (Pair<U, Set<V>> childPair : child.pairs) {
                assert compareFunction.apply(childPair.getFirst().compareTo(pair.getFirst()));
            }
        }

    }
}
