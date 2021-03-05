package sample.model;

import sample.model.pojo.BinarySearchDirection;
import sample.model.pojo.ICondition;
import sample.model.pojo.Pair;
import sample.model.pojo.SimpleCondition;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class BinaryTree<U extends Comparable, V> implements FieldKeeper<U, V> {
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
    public void transform(U oldKey, U key, V value) {

    }

    @Override
    public void insert(U key, V value) {
        LockService.doInComparableLock(key, () -> {
            final ChainComparableLock chainComparableLock = new ChainComparableLock();
            final Pair<Node<V>, Node<V>> pair;
            try {
                pair = search(root, key, chainComparableLock);
            } finally {
                chainComparableLock.close();
            }
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
                    createdNode.parent = pair.getSecond();
                    return null;
                });
            } else {
                pair.getFirst().value.add(value);
            }
            return null;
        });
    }

    @Override
    public void delete(U key, V value) {

    }

    @Override
    public Set<V> search(SimpleCondition condition) {
        final ChainComparableLock chainComparableLock = new ChainComparableLock();
        synchronized (ROOT_LOCK) {
            if (root == null) {
                return Collections.emptySet();
            }
            chainComparableLock.lock(root.key);
        }
        try {
            final ConditionSearcher<V> conditionSearcher = new ConditionSearcher<>(condition, chainComparableLock);
            conditionSearcher.search(root);
            return conditionSearcher.set;
        } finally {
            chainComparableLock.close();
        }
    }

    @Override
    public Set<V> search(U comparable) {
        final ChainComparableLock chainComparableLock = new ChainComparableLock();
        synchronized (ROOT_LOCK) {
            if (root == null) {
                return Collections.emptySet();
            }
            chainComparableLock.lock(root.key);
        }
        try {
            final Pair<Node<V>, Node<V>> pair = search(root, comparable, chainComparableLock);
            if (pair == null || pair.getFirst() == null) {
                return Collections.emptySet();
            }
            return pair.getFirst().value;
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
            if (node == null) {
                return;
            }
            if (conditionService.check(node.key, condition)) {
                set.addAll(node.value);
            }
            if (node.left == null && node.right == null) {
                return;
            }
            final BinarySearchDirection searchDirection = determineDirection(node.key);
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

        private BinarySearchDirection determineDirection(Comparable value) {
            if (value == null) {
                throw new ConditionException("unknown field " + condition.getField());
            }
            if (ICondition.SimpleType.LIKE.equals(condition.getType())) {
                if (condition.getValue() == null) {
                    return BinarySearchDirection.NONE;
                }
                return BinarySearchDirection.BOTH;
            }
            final int compareResult = value.compareTo(condition.getValue());
            switch (condition.getType()) {
                case EQ:
                    if (compareResult == 0) {
                        return BinarySearchDirection.NONE;
                    } else if (compareResult > 0) {
                        return BinarySearchDirection.LEFT;
                    }
                    return BinarySearchDirection.RIGHT;
                case NOT:
                    return BinarySearchDirection.BOTH;
                case GT:
                case GTE:
                    if (compareResult > 0) {
                        return BinarySearchDirection.BOTH;
                    }
                    return BinarySearchDirection.RIGHT;
                case LT:
                case LTE:
                    if (compareResult < 0) {
                        return BinarySearchDirection.BOTH;
                    }
                    return BinarySearchDirection.LEFT;
                default:
                    throw new ConditionException("Unknown simple type : " + condition.getType());
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

    private Pair<Node<V>, Node<V>> search(Node<V> node, Comparable key, ChainComparableLock chainComparableLock) {
        if (key == null) {
            throw new RuntimeException("null key");
        }
        if (node == null) {
            return new Pair<>(null, null);
        }
        chainComparableLock.lock(key);
        final int compareResult = key.compareTo(node.key);
        if (compareResult == 0) {
            return new Pair<>(node, null);
        } else if (compareResult < 0) {
            if (node.left == null) {
                return new Pair<>(null, node);
            }
            return search(node.left, key, chainComparableLock);
        }
        if (node.right == null) {
            return new Pair<>(null, node);
        }
        return search(node.right, key, chainComparableLock);
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
