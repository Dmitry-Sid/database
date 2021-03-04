package sample.model;

import sample.model.pojo.BinarySearchDirection;
import sample.model.pojo.Pair;
import sample.model.pojo.SimpleCondition;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class BinaryTree<V> implements FieldKeeper<V> {
    private final Object ROOT_LOCK = new Object();
    private final ConditionService conditionService;
    private final String field;
    private Node<V> root;

    public BinaryTree(ConditionService conditionService, String field) {
        this.conditionService = conditionService;
        this.field = field;
    }

    @Override
    public String getField() {
        return field;
    }

    @Override
    public void transform(Comparable oldKey, Comparable key, V value) {

    }

    @Override
    public void insert(Comparable key, V value) {
        LockService.doInComparableLock(key, () -> {
            final Pair<Node<V>, Node<V>> pair = search(root, key);
            if (pair.getFirst() == null) {
                final Set<V> set = new HashSet<>();
                set.add(value);
                final Node<V> createdNode = new Node<>(key, set);
                synchronized (ROOT_LOCK) {
                    if (root == null) {
                        root = createdNode;
                        return null;
                    }
                }
                LockService.doInComparableLock(pair.getSecond().key, () -> {
                    if (key.compareTo(pair.getSecond().key) > 0) {
                        pair.getSecond().right = createdNode;
                    } else {
                        pair.getSecond().left = createdNode;
                    }
                    return null;
                });
            } else {
                pair.getFirst().value.add(value);
            }
            return null;
        });
    }

    @Override
    public void delete(Comparable key, V value) {

    }

    @Override
    public Set<V> search(SimpleCondition condition) {
        synchronized (ROOT_LOCK) {
            if (root == null) {
                return Collections.emptySet();
            }
        }
        final ChainComparableLock chainComparableLock = new ChainComparableLock();
        chainComparableLock.lock(root.key);
        try {
            final ConditionSearcher<V> conditionSearcher = new ConditionSearcher<>(condition, chainComparableLock);
            conditionSearcher.search(root);
            return conditionSearcher.set;
        } finally {
            chainComparableLock.close();
        }
    }

    private class ConditionSearcher<V> {
        private final Set<V> set = new HashSet<>();
        private final SimpleCondition condition;
        private final ChainComparableLock chainComparableLock;

        private ConditionSearcher(SimpleCondition condition, ChainComparableLock chainComparableLock) {
            this.condition = condition;
            this.chainComparableLock = chainComparableLock;
        }

        private void search(Node<V> node) {
            if (conditionService.check(node.key, condition)) {
                set.addAll(node.value);
            }
            if (node.left == null && node.right == null) {
                return;
            }
            final BinarySearchDirection searchDirection = conditionService.determineDirection(node.key, condition);
            if (BinarySearchDirection.NONE.equals(searchDirection)) {
                return;
            }
            if ((BinarySearchDirection.LEFT.equals(searchDirection) || BinarySearchDirection.BOTH.equals(searchDirection))
                    && node.left != null) {
                chainComparableLock.lock(node.left.key);
                search(node.left);
            }
            if ((BinarySearchDirection.RIGHT.equals(searchDirection) || BinarySearchDirection.BOTH.equals(searchDirection))
                    && node.right != null) {
                chainComparableLock.lock(node.right.key);
                search(node.right);
            }
        }
    }

    private class ChainComparableLock {
        private Comparable currentComparable;

        private void lock(Comparable comparable) {
            close();
            currentComparable = comparable;
            LockService.getComparableLock().lock(comparable);
        }

        private void close() {
            if (currentComparable != null) {
                LockService.getComparableLock().unlock(currentComparable);
            }
        }
    }

    private Pair<Node<V>, Node<V>> search(Node<V> node, Comparable key) {
        if (key == null) {
            throw new RuntimeException("null key");
        }
        if (node == null) {
            return null;
        }
        return LockService.doInComparableLock(key, () -> {
            final int compareResult = key.compareTo(node);
            if (compareResult == 0) {
                return new Pair<>(node, null);
            } else if (compareResult < 0) {
                final Pair<Node<V>, Node<V>> pair = search(node.left, key);
                if (node.left == null && node.right == null) {
                    return new Pair<>(null, node);
                }
                return pair;
            }
            final Pair<Node<V>, Node<V>> pair = search(node.right, key);
            if (node.left == null && node.right == null) {
                return new Pair<>(null, node);
            }
            return pair;
        });
    }

    private static class Node<V> {
        private final Comparable key;
        private final Set<V> value;
        private Node<V> parent;
        private Node<V> left;
        private Node<V> right;

        public Node(Comparable key, Set<V> value) {
            this.key = key;
            this.value = value;
        }

    }
}
