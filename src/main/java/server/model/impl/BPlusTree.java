package server.model.impl;

import server.model.BaseFieldKeeper;
import server.model.ConditionService;
import server.model.Destroyable;
import server.model.ObjectConverter;
import server.model.lock.LockService;
import server.model.pojo.Pair;
import server.model.pojo.SimpleCondition;

import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;

public class BPlusTree<U extends Comparable<U>, V> extends BaseFieldKeeper<U, V> {
    protected final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    public final int treeFactor;

    public BPlusTree(String fieldName, String path, ObjectConverter objectConverter, ConditionService conditionService, int treeFactor) {
        super(fieldName, path, objectConverter, conditionService);
        this.treeFactor = treeFactor;
    }

    @Override
    protected Variables<U, V> createVariables() {
        return new BTreeVariables<>((LeafNode<U, V>) createNode(LeafNode.class), new AtomicLong(Long.MIN_VALUE));
    }

    @Override
    public void insertNotNull(U key, V value) {
        LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Write, () -> {
            if (getVariables().root.pairs.size() != treeFactor * 2 - 1) {
                insert(getVariables().root, key, value);
                return;
            }
            final InternalNode<U, V> created = createNode(InternalNode.class);
            final Node<U, V> root = getVariables().root;
            getVariables().root = created;
            save(root);
            created.children.add(root);
            split(created, 0);
            insert(created, key, value);
        });
    }

    private List<Node<U, V>> getChildren(Node<U, V> node) {
        if (isLeaf(node)) {
            throw new IllegalStateException("leaf cannot has children");
        }
        return ((InternalNode<U, V>) node).children;
    }

    private void insert(Node<U, V> node, U key, V value) {
        int index = getChildIndex(node, key);
        if (index > 0) {
            final Pair<U, Set<V>> pair = node.pairs.get(index - 1);
            if (key.compareTo(pair.getFirst()) == 0) {
                pair.getSecond().add(value);
                save(node);
                return;
            }
        }
        if (isLeaf(node)) {
            node.pairs.add(index, new Pair<>(key, new HashSet<>(Collections.singleton(value))));
            save(node);
            return;
        }
        final Node<U, V> child = readChild(node, index);
        if (child.pairs.size() == 2 * treeFactor - 1) {
            split((InternalNode<U, V>) node, index);
            if (key.compareTo(node.pairs.get(index).getFirst()) > 0) {
                index++;
            }
        }
        insert(readChild(node, index), key, value);
    }

    private int getChildIndex(Node<U, V> node, U key) {
        return find(node.pairs, key, compareResult -> compareResult < 0).getFirst();
    }

    private void split(InternalNode<U, V> node, int index) {
        final Node<U, V> left = readChild(node, index);
        final Node<U, V> right = createNode(left.getClass());
        rearrange(left.pairs, right.pairs, treeFactor);
        if (!isLeaf(left)) {
            rearrange(getChildren(left), getChildren(right), treeFactor);
        }
        node.pairs.add(index, left.pairs.get(left.pairs.size() - 1));
        left.pairs.remove(left.pairs.size() - 1);
        node.children.add(index + 1, right);
        save(node);
        save(left);
        save(right);
    }

    private <T> void rearrange(List<T> listFrom, List<T> listTo, int indexFrom) {
        int i = 0;
        for (Iterator<T> iterator = listFrom.iterator(); iterator.hasNext(); ) {
            final T element = iterator.next();
            if (i >= indexFrom) {
                listTo.add(element);
                iterator.remove();
            }
            i++;
        }
    }

    @Override
    public DeleteResult deleteNotNull(U key, V value) {
        return LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Write, () -> {
            final Pair<Node<U, V>, Pair<U, Set<V>>> pair = search(getVariables().root, key);
            if (!pair.getSecond().getSecond().contains(value)) {
                return NOT;
            }
            pair.getSecond().getSecond().remove(value);
            save(pair.getFirst());
            if (pair.getSecond().getSecond().isEmpty()) {
                delete(getVariables().root, key);
                return FULLY;
            }
            return NOT_FULLY;
        });
    }

    private void delete(Node<U, V> node, U key) {
        final Pair<Integer, Boolean> indexPair = find(node.pairs, key, compareResult -> compareResult == 0);
        if (indexPair.getSecond()) {
            if (isLeaf(node)) {
                node.pairs.remove(indexPair.getFirst().intValue());
                save(node);
                return;
            }
            Node<U, V> childLeft = readChild(node, indexPair.getFirst());
            if (childLeft.pairs.size() >= treeFactor) {
                move(childLeft, node, findPredecessor(childLeft, key), indexPair.getFirst());
            } else {
                final Node<U, V> childRight = readChild(node, indexPair.getFirst() + 1);
                if (childRight.pairs.size() >= treeFactor) {
                    move(childRight, node, findSuccessor(childRight, key), indexPair.getFirst());
                } else {
                    final Pair<U, Set<V>> pair = node.pairs.get(indexPair.getFirst());
                    merge((InternalNode<U, V>) node, childLeft, childRight, indexPair.getFirst() + 1);
                    delete(read(childLeft), pair.getFirst());
                    if (node.pairs.isEmpty()) {
                        getVariables().root = read(childLeft);
                    }
                }
            }
            return;
        }
        if (isLeaf(node)) {
            return;
        }
        final InternalNode<U, V> internalNode = (InternalNode<U, V>) node;
        if (internalNode.children.size() == 0) {
            return;
        }
        final int childIndex = getChildIndex(internalNode, key);
        Node<U, V> child = readChild(internalNode, childIndex);
        if (child.pairs.size() == treeFactor - 1) {
            Node<U, V> childLeft = null;
            Node<U, V> childRight = null;
            boolean moved = false;
            if (childIndex > 0) {
                childLeft = readChild(internalNode, childIndex - 1);
                if (childLeft.pairs.size() >= treeFactor) {
                    child = lend(internalNode, child, childLeft, childIndex, true);
                    moved = true;
                }
            }
            if (!moved) {
                if (childIndex < internalNode.children.size() - 1) {
                    childRight = readChild(internalNode, (childIndex + 1));
                    if (childRight.pairs.size() >= treeFactor) {
                        child = lend(internalNode, child, childRight, childIndex, false);
                        moved = true;
                    }
                }
            }
            if (!moved && childLeft != null && childLeft.pairs.size() == treeFactor - 1) {
                child = merge(internalNode, childLeft, child, childIndex);
                moved = true;
            }
            if (!moved && childRight != null && childRight.pairs.size() == treeFactor - 1) {
                child = merge(internalNode, child, childRight, childIndex + 1);
            }
        }
        delete(read(child), key);
    }

    private void delete(Node<U, V> node) {
        if (!isLeaf(node)) {
            return;
        }
        new File(((LeafNode<U, V>) node).fileName).delete();
    }

    private Node<U, V> merge(InternalNode<U, V> node, Node<U, V> childLeft, Node<U, V> childRight, int index) {
        node.children.remove(index);
        childLeft.pairs.add(node.pairs.remove(index - 1));
        childLeft.pairs.addAll(childRight.pairs);
        if (!isLeaf(childLeft)) {
            getChildren(childLeft).addAll(getChildren(childRight));
        }
        if (node.pairs.isEmpty()) {
            getVariables().root = childLeft;
        }
        save(childLeft);
        save(childRight);
        save(node);
        return read(childLeft);
    }

    private Pair<Integer, Boolean> find(List<Pair<U, Set<V>>> pairs, U key, Function<Integer, Boolean> compareFunction) {
        for (int i = 0; i < pairs.size(); i++) {
            final Pair<U, Set<V>> pair = pairs.get(i);
            if (compareFunction.apply(key.compareTo(pair.getFirst()))) {
                return new Pair<>(i, true);
            }
        }
        return new Pair<>(pairs.size(), false);
    }

    private void move(Node<U, V> nodeFrom, Node<U, V> nodeTo, Pair<U, Set<V>> pair, int indexTo) {
        delete(nodeFrom, pair.getFirst());
        final Node<U, V> readNode = read(nodeTo);
        readNode.pairs.set(indexTo, pair);
        save(readNode);
    }

    private Node<U, V> lend(Node<U, V> node, Node<U, V> child, Node<U, V> donor, int index, boolean left) {
        final Pair<U, Set<V>> nodePair = node.pairs.get(left ? index - 1 : index);
        final Pair<U, Set<V>> donorPair = donor.pairs.remove(left ? donor.pairs.size() - 1 : 0);
        child.pairs.add(left ? 0 : child.pairs.size(), nodePair);
        node.pairs.set(left ? index - 1 : index, donorPair);
        if (!isLeaf(child)) {
            getChildren(child).add(left ? 0 : getChildren(child).size(), getChildren(donor).remove(left ? getChildren(donor).size() - 1 : 0));
        }
        save(node);
        save(child);
        save(donor);
        return read(child);
    }

    private Pair<U, Set<V>> findPredecessor(Node<U, V> node, U key) {
        final Pair<Integer, Boolean> pair = find(node.pairs, key, compareResult -> compareResult < 0);
        if (isLeaf(node)) {
            return node.pairs.get(pair.getFirst() - 1);
        }
        return findPredecessor(read(getChildren(node).get(pair.getFirst() != node.pairs.size() ? pair.getFirst() - 1 : pair.getFirst())), key);
    }

    private Pair<U, Set<V>> findSuccessor(Node<U, V> node, U key) {
        final Pair<Integer, Boolean> pair = find(node.pairs, key, compareResult -> compareResult < 0);
        if (isLeaf(node)) {
            return node.pairs.get(pair.getFirst());
        }
        return findSuccessor(read(getChildren(node).get(pair.getFirst())), key);
    }

    @Override
    public Set<V> searchNotNull(SimpleCondition condition) {
        return LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Read, (Supplier<Set<V>>) Collections::emptySet);
    }

    @Override
    public Set<V> searchNotNull(U key) {
        return LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Read, () -> search(getVariables().root, key).getSecond().getSecond());
    }

    @Override
    public void clear() {
        LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Write, () -> {
            final String searchPath;
            if (new File(path).isDirectory()) {
                searchPath = path;
            } else {
                searchPath = System.getProperty("user.dir");
            }
            for (File file : Objects.requireNonNull(new File(searchPath).listFiles((file, name) -> name.endsWith(fieldName)))) {
                file.delete();
            }
        });
    }

    @Override
    public void destroy() {
        LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Write, super::destroy);
    }

    private Pair<Node<U, V>, Pair<U, Set<V>>> search(Node<U, V> node, U key) {
        if (node == null || node.pairs.isEmpty()) {
            return new Pair<>(null, new Pair<>(null, Collections.emptySet()));
        }
        Pair<U, Set<V>> pair = null;
        int index = 0;
        for (; index < node.pairs.size(); index++) {
            pair = node.pairs.get(index);
            if (key.compareTo(pair.getFirst()) <= 0) {
                break;
            }
        }
        if (pair.getFirst().compareTo(key) == 0) {
            return new Pair<>(node, pair);
        } else if (isLeaf(node)) {
            return new Pair<>(node, new Pair<>(null, Collections.emptySet()));
        }
        return search(readChild(node, index), key);
    }

    protected Node<U, V> readChild(Node<U, V> node, int index) {
        if (isLeaf(node)) {
            return null;
        }
        return read(getChildren(node).get(index));
    }

    private Node<U, V> read(Node<U, V> node) {
        if (node instanceof InternalNode) {
            return node;
        }
        LeafNode<U, V> leafNode = (LeafNode<U, V>) node;
        if (!isInitialized(leafNode)) {
            leafNode = objectConverter.fromFile(LeafNode.class, ((LeafNode) node).fileName);
        }
        leafNode.init();
        return leafNode;
    }

    private <T extends Node<U, V>> T createNode(Class<T> clazz) {
        if (LeafNode.class.equals(clazz)) {
            final LeafNode<U, V> leafNode = new LeafNode<>(getFileName(getVariables() == null ? Long.MIN_VALUE : getVariables().counter.incrementAndGet()));
            leafNode.init();
            return (T) leafNode;
        }
        return (T) new InternalNode<U, V>();
    }

    private void save(Node<U, V> node) {
        if (node == null || !isLeaf(node) || node == getVariables().root) {
            return;
        }
        final LeafNode<U, V> leafNode = (LeafNode<U, V>) node;
        if (leafNode.pairs.isEmpty()) {
            delete(leafNode);
            return;
        }
        objectConverter.toFile(node, leafNode.fileName);
        leafNode.destroy();
    }

    protected boolean isLeaf(Node<U, V> node) {
        return node instanceof LeafNode;
    }

    protected boolean isInitialized(Node<U, V> node) {
        if (isLeaf(node)) {
            return ((LeafNode<U, V>) node).initialized;
        }
        return false;
    }

    /**
     * protected Для тестов
     */
    protected BTreeVariables<U, V> getVariables() {
        return (BTreeVariables<U, V>) variables;
    }

    public static class BTreeVariables<U, V> extends Variables<U, V> {
        private static final long serialVersionUID = -4536650721479536430L;
        private final AtomicLong counter;
        public volatile Node<U, V> root;

        private BTreeVariables(Node<U, V> root, AtomicLong counter) {
            this.root = root;
            this.counter = counter;
        }
    }

    public static abstract class Node<U, V> implements Serializable {
        public volatile List<Pair<U, Set<V>>> pairs;
    }

    public static class InternalNode<U, V> extends Node<U, V> {
        private static final long serialVersionUID = 1083626043070239192L;
        public final List<Node<U, V>> children = Collections.synchronizedList(new ArrayList<>());

        public InternalNode() {
            pairs = Collections.synchronizedList(new ArrayList<>());
        }
    }

    public static class LeafNode<U, V> extends Node<U, V> implements Destroyable {
        private static final long serialVersionUID = -4761087948139648759L;
        public final String fileName;
        private volatile boolean initialized = false;

        public LeafNode(String fileName) {
            this.fileName = fileName;
        }

        private void init() {
            if (!initialized) {
                pairs = new ArrayList<>();
                initialized = true;
            }
        }

        @Override
        public void destroy() {
            pairs = null;
            initialized = false;
        }
    }
}
