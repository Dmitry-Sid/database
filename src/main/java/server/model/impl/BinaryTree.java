package server.model.impl;

import server.model.BaseFieldKeeper;
import server.model.ConditionException;
import server.model.ConditionService;
import server.model.ObjectConverter;
import server.model.lock.LockService;
import server.model.pojo.ICondition;
import server.model.pojo.Pair;
import server.model.pojo.SimpleCondition;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BinaryTree<U extends Comparable<U>, V> extends BaseFieldKeeper<U, V> {
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
        LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Write, () -> {
            final Pair<Node<U, V>, Node<U, V>> pair;
            pair = search(getVariables().root, key);
            if (pair.getFirst() == null) {
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
                pair.getFirst().value.add(value);
            }
        });
    }

    @Override
    public DeleteResult deleteNotNull(U key, V value) {
        return LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Write, () -> {
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
    public Set<V> conditionSearchNotNull(SimpleCondition condition) {
        return LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Read, () -> {
            if (getVariables().root == null) {
                return Collections.emptySet();
            }
            final ConditionSearcher conditionSearcher = new ConditionSearcher(condition);
            conditionSearcher.search(getVariables().root);
            return conditionSearcher.set;
        });
    }

    @Override
    public Set<V> searchNotNull(U key) {
        return LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Read, () -> {
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

    private enum BinarySearchDirection {
        LEFT, RIGHT, BOTH, NONE
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
        private final Set<V> set = new HashSet<>();
        private final SimpleCondition condition;

        private ConditionSearcher(SimpleCondition condition) {
            this.condition = condition;
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
                search(node.left);
            }
            if ((BinarySearchDirection.RIGHT.equals(searchDirection) || BinarySearchDirection.BOTH.equals(searchDirection))
                    && node.right != null) {
                search(node.right);
            }
        }

        private BinarySearchDirection determineDirection(U value) {
            if (value == null) {
                throw new ConditionException("unknown field " + condition.getField());
            }
            if (condition.getValue() == null) {
                return SimpleCondition.SimpleType.EQ.equals(condition.getType()) ? BinarySearchDirection.NONE : BinarySearchDirection.BOTH;
            }
            if (ICondition.SimpleType.LIKE.equals(condition.getType())) {
                if (condition.getValue() == null) {
                    return BinarySearchDirection.NONE;
                }
                return BinarySearchDirection.BOTH;
            }
            final int compareResult = value.compareTo((U) condition.getValue());
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
}
