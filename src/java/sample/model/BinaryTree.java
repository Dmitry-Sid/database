package sample.model;

import sample.model.pojo.BinarySearchDirection;
import sample.model.pojo.ICondition;
import sample.model.pojo.Pair;
import sample.model.pojo.SimpleCondition;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

public class BinaryTree<U extends Comparable, V> extends FieldKeeper<U, V> {
    private final Lock<Comparable> lock = LockService.createLock(Comparable.class);
    private final Object ROOT_LOCK = new Object();
    private Node<U, V> root;

    public BinaryTree(String field, ConditionService conditionService) {
        super(field, conditionService);
    }

    @Override
    public void insert(U key, V value) {
        LockService.doInLock(lock, key, () -> {
            final Pair<Node<U, V>, Node<U, V>> pair;
            final ChainComparableLock chainComparableLock = new ChainComparableLock();
            try {
                pair = search(root, key, chainComparableLock);
            } finally {
                chainComparableLock.close();
            }
            if (pair.getFirst() == null) {
                final Node<U, V> createdNode = new Node<>(key, new HashSet<>(Collections.singletonList(value)));
                synchronized (ROOT_LOCK) {
                    if (root == null) {
                        root = createdNode;
                        return null;
                    }
                }
                LockService.doInLock(lock, pair.getSecond().key, () -> {
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
    public boolean delete(U key, V value) {
        return LockService.doInLock(lock, key, () -> {
            final ChainComparableLock chainComparableLock = new ChainComparableLock();
            synchronized (ROOT_LOCK) {
                if (root == null) {
                    return false;
                }
                chainComparableLock.lock(root.key);
            }
            final Node<U, V> node;
            try {
                node = search(root, key, chainComparableLock).getFirst();
            } finally {
                chainComparableLock.close();
            }
            if (node == null) {
                return false;
            }
            if (!node.value.contains(value)) {
                return false;
            }
            node.value.remove(value);
            if (!node.value.isEmpty()) {
                return true;
            }
            if (node.left == null) {
                rearrange(node, node.right);
            } else if (node.right == null) {
                rearrange(node, node.left);
            } else {
                try {
                    final Node<U, V> minimum = findMinimum(node.right, chainComparableLock);
                    if (minimum.parent != node) {
                        rearrange(minimum, minimum.right);
                        minimum.right = node.right;
                        minimum.right.parent = minimum;
                    }
                    rearrange(node, minimum);
                    minimum.left = node.left;
                    minimum.left.parent = minimum;
                } finally {
                    chainComparableLock.close();
                }
            }
            return true;
        });
    }

    private void rearrange(Node<U, V> nodeFrom, Node<U, V> nodeTo) {
        if (nodeFrom == null) {
            return;
        }
        LockService.doInLock(lock, nodeFrom.key, () -> {
            final Supplier<Object> supplier = () -> {
                synchronized (ROOT_LOCK) {
                    if (nodeFrom == root) {
                        root = nodeTo;
                        return null;
                    }
                }
                final boolean changed = LockService.doInLock(lock, nodeFrom.parent.left.key, () -> {
                    if (nodeFrom == nodeFrom.parent.left) {
                        nodeFrom.parent.left = nodeTo;
                        return true;
                    }
                    return false;
                });
                if (!changed) {
                    LockService.doInLock(lock, nodeFrom.parent.right.key, () -> {
                        nodeFrom.parent.right = nodeTo;
                        return null;
                    });
                }
                if (nodeTo != null) {
                    LockService.doInLock(lock, nodeFrom.parent.key, () -> {
                        nodeTo.parent = nodeFrom.parent;
                        return null;
                    });
                }
                return null;
            };
            if (nodeTo != null) {
                LockService.doInLock(lock, nodeTo.key, supplier::get);
            } else {
                return supplier.get();
            }
            return null;
        });
    }

    private Node<U, V> findMinimum(Node<U, V> parent, ChainComparableLock chainComparableLock) {
        if (parent == null) {
            return null;
        }
        chainComparableLock.lock(parent.key);
        if (parent.left == null) {
            return parent;
        }
        return findMinimum(parent.left, chainComparableLock);
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
    public Set<V> search(U key) {
        final ChainComparableLock chainComparableLock = new ChainComparableLock();
        synchronized (ROOT_LOCK) {
            if (root == null) {
                return Collections.emptySet();
            }
            chainComparableLock.lock(root.key);
        }
        try {
            final Pair<Node<U, V>, Node<U, V>> pair = search(root, key, chainComparableLock);
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

        private void search(Node<U, V> node) {
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
        private U currentComparable;

        private void lock(U comparable) {
            close();
            currentComparable = comparable;
            lock.lock(comparable);
        }

        private void close() {
            if (currentComparable != null) {
                lock.unlock(currentComparable);
            }
        }
    }

    private Pair<Node<U, V>, Node<U, V>> search(Node<U, V> node, U key, ChainComparableLock chainComparableLock) {
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

    private static class Node<U, V> {
        private final U key;
        private final Set<V> value;
        private Node<U, V> parent;
        private Node<U, V> left;
        private Node<U, V> right;

        public Node(U key, Set<V> value) {
            this.key = key;
            this.value = value;
        }

    }
}
