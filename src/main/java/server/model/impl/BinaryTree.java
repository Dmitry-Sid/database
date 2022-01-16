package server.model.impl;

import server.model.BaseFieldKeeper;
import server.model.ConditionService;
import server.model.ObjectConverter;
import server.model.Utils;
import server.model.lock.LockService;
import server.model.pojo.FieldCondition;
import server.model.pojo.ICondition;
import server.model.pojo.Pair;
import server.model.pojo.SimpleCondition;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BinaryTree<U extends Comparable<U>, V> extends BaseFieldKeeper<U, V> {
    private static final Collection<SearchDirection> BOTH = Collections.unmodifiableList(Arrays.asList(SearchDirection.LEFT, SearchDirection.RIGHT));
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public BinaryTree(String field, String path, ObjectConverter objectConverter, ConditionService conditionService) {
        super(field, path, objectConverter, conditionService);
    }

    @Override
    protected BaseFieldKeeper.Variables<U, V> createVariables() {
        return new BinaryTreeVariables<>(null);
    }

    @Override
    public void insertNotNull(U key, V value) {
        LockService.doInReadWriteLock(readWriteLock.writeLock(), () -> {
            final Pair<Node<U, V>, Node<U, V>> pair;
            pair = search(getVariables().root, key);
            if (pair.getFirst() == null) {
                changed = true;
                final Node<U, V> createdNode = new Node<>(key, new HashSet<>(Collections.singletonList(value)));
                if (getVariables().root == null) {
                    getVariables().root = createdNode;
                    return;
                }
                if (key.compareTo(pair.getSecond().key) > 0) {
                    pair.getSecond().right = createdNode;
                } else {
                    pair.getSecond().left = createdNode;
                }
                createdNode.parent = pair.getSecond();
            } else {
                final int size = pair.getFirst().value.size();
                pair.getFirst().value.add(value);
                if (size != pair.getFirst().value.size()) {
                    changed = true;
                }
            }
        });
    }

    @Override
    public DeleteResult deleteNotNull(U key, V value) {
        return LockService.doInReadWriteLock(readWriteLock.writeLock(), () -> {
            if (getVariables().root == null) {
                return NOT;
            }
            final Node<U, V> node = search(getVariables().root, key).getFirst();
            if (node == null) {
                return NOT;
            }
            if (!node.value.contains(value)) {
                return NOT;
            }
            node.value.remove(value);
            if (!node.value.isEmpty()) {
                return NOT_FULLY;
            }
            if (node.left == null) {
                rearrange(node, node.right);
            } else if (node.right == null) {
                rearrange(node, node.left);
            } else {
                final Node<U, V> minimum = findMinimum(node.right);
                if (minimum.parent != node) {
                    rearrange(minimum, minimum.right);
                    minimum.right = node.right;
                    minimum.right.parent = minimum;
                }
                rearrange(node, minimum);
                minimum.left = node.left;
                minimum.left.parent = minimum;
            }
            return FULLY;
        });
    }

    private void rearrange(Node<U, V> nodeFrom, Node<U, V> nodeTo) {
        if (nodeFrom == null) {
            return;
        }
        if (nodeFrom == getVariables().root) {
            getVariables().root = nodeTo;
            return;
        }
        final boolean changed = nodeFrom == nodeFrom.parent.left;
        if (changed) {
            nodeFrom.parent.left = nodeTo;
            return;
        } else {
            nodeFrom.parent.right = nodeTo;
        }
        if (nodeTo != null) {
            nodeTo.parent = nodeFrom.parent;
        }
    }

    private Node<U, V> findMinimum(Node<U, V> parent) {
        if (parent == null) {
            return null;
        }
        if (parent.left == null) {
            return parent;
        }
        return findMinimum(parent.left);
    }

    @Override
    public void conditionSearchNotNull(FieldCondition condition, Set<V> set, int size) {
        LockService.doInReadWriteLock(readWriteLock.readLock(), () -> {
            if (getVariables().root == null) {
                return;
            }
            new ConditionSearcher(condition, set, size).search(getVariables().root);
        });
    }

    @Override
    public Set<V> searchNotNull(U key) {
        return LockService.doInReadWriteLock(readWriteLock.readLock(), () -> {
            if (getVariables().root == null) {
                return Collections.emptySet();
            }
            final Pair<Node<U, V>, Node<U, V>> pair = search(getVariables().root, key);
            if (pair == null || pair.getFirst() == null) {
                return Collections.emptySet();
            }
            return pair.getFirst().value;
        });
    }

    @Override
    public void destroy() {
        LockService.doInReadWriteLock(readWriteLock.writeLock(), super::destroy);
    }

    private Pair<Node<U, V>, Node<U, V>> search(Node<U, V> node, U key) {
        if (node == null) {
            return new Pair<>(null, null);
        }
        final int compareResult = key.compareTo(node.key);
        if (compareResult == 0) {
            return new Pair<>(node, null);
        } else if (compareResult < 0) {
            if (node.left == null) {
                return new Pair<>(null, node);
            }
            return search(node.left, key);
        }
        if (node.right == null) {
            return new Pair<>(null, node);
        }
        return search(node.right, key);
    }

    private BinaryTreeVariables<U, V> getVariables() {
        return (BinaryTreeVariables<U, V>) variables;
    }

    private enum SearchDirection {
        LEFT, RIGHT, NONE
    }

    private static class Node<U, V> implements Serializable {
        private static final long serialVersionUID = 1580600256004817186L;
        private final U key;
        private final Set<V> value;
        private Node<U, V> parent;
        private Node<U, V> left;
        private Node<U, V> right;

        Node(U key, Set<V> value) {
            this.key = key;
            this.value = value;
        }

    }

    private static class BinaryTreeVariables<U, V> extends Variables<U, V> {
        private static final long serialVersionUID = -5170318601024702402L;
        private Node<U, V> root;

        private BinaryTreeVariables(Node<U, V> root) {
            this.root = root;
        }
    }

    private class ConditionSearcher {
        private final FieldCondition condition;
        private final Set<V> set;
        private final int size;

        private ConditionSearcher(FieldCondition condition, Set<V> set, int size) {
            this.condition = condition;
            this.set = set;
            this.size = size;
        }

        private void search(Node<U, V> node) {
            if (node == null || Utils.isFull(set, size)) {
                return;
            }
            if (conditionService.check(node.key, condition)) {
                if (Utils.fillToFull(set, size, node.value)) {
                    return;
                }
            }
            if (node.left == null && node.right == null) {
                return;
            }
            final Set<SearchDirection> searchDirections = Utils.collectConditions(condition, condition -> determineDirections(condition, node.key));
            if (searchDirections.contains(SearchDirection.NONE)) {
                return;
            }
            if (searchDirections.contains(SearchDirection.LEFT) && node.left != null) {
                search(node.left);
            }
            if (searchDirections.contains(SearchDirection.RIGHT) && node.right != null) {
                search(node.right);
            }
        }

        private Collection<SearchDirection> determineDirections(SimpleCondition condition, U value) {
            if (condition.getValue() == null) {
                return SimpleCondition.SimpleType.EQ.equals(condition.getType()) ? Collections.singletonList(SearchDirection.NONE) : BOTH;
            }
            if (ICondition.SimpleType.LIKE.equals(condition.getType())) {
                if (condition.getValue() == null) {
                    return Collections.singletonList(SearchDirection.NONE);
                }
                return BOTH;
            }
            final int compareResult = value.compareTo((U) condition.getValue());
            switch (condition.getType()) {
                case EQ:
                    if (compareResult == 0) {
                        return Collections.singletonList(SearchDirection.NONE);
                    } else if (compareResult > 0) {
                        return Collections.singletonList(SearchDirection.LEFT);
                    }
                    return Collections.singletonList(SearchDirection.RIGHT);
                case NOT:
                    return BOTH;
                case GT:
                case GTE:
                    if (compareResult > 0) {
                        return BOTH;
                    }
                    return Collections.singletonList(SearchDirection.RIGHT);
                case LT:
                case LTE:
                    if (compareResult < 0) {
                        return BOTH;
                    }
                    return Collections.singletonList(SearchDirection.LEFT);
                default:
                    throw new IllegalArgumentException("Unknown simple type : " + condition.getType());
            }
        }
    }
}
