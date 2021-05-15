package server.model.impl;

import server.model.BaseFieldKeeper;
import server.model.ConditionService;
import server.model.Destroyable;
import server.model.ObjectConverter;
import server.model.pojo.Pair;
import server.model.pojo.SimpleCondition;

import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class BPlusTree<U extends Comparable<U>, V> extends BaseFieldKeeper<U, V> {
    public final int treeFactor;

    public BPlusTree(String fieldName, String path, ObjectConverter objectConverter, int treeFactor) {
        super(fieldName, path, objectConverter);
        this.treeFactor = treeFactor;
    }

    @Override
    protected Variables<U, V> createVariables() {
        return new BTreeVariables<>((LeafNode<U, V>) createNode(LeafNode.class), new AtomicLong(Long.MIN_VALUE));
    }

    @Override
    public synchronized void insertNotNull(U key, V value) {
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
        final Pair<Integer, Pair<U, Set<V>>> pair = find(node.pairs, key, compareResult -> compareResult < 0);
        if (pair != null) {
            return pair.getFirst();
        }
        return node.pairs.size();
    }

    private synchronized void split(InternalNode<U, V> node, int index) {
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
    public synchronized DeleteResult deleteNotNull(U key, V value) {
        final Pair<Node<U, V>, Set<V>> pair = search(getVariables().root, key);
        if (!pair.getSecond().contains(value)) {
            return NOT;
        }
        pair.getSecond().remove(value);
        save(pair.getFirst());
        if (pair.getSecond().isEmpty()) {
            delete(getVariables().root, key);
            return FULLY;
        }
        return NOT_FULLY;
    }

    private synchronized void delete(Node<U, V> node, U key) {
        final Pair<Integer, Pair<U, Set<V>>> pair = find(node.pairs, key, compareResult -> compareResult == 0);
        if (pair != null) {
            final int index = pair.getFirst();
            if (isLeaf(node)) {
                node.pairs.remove(index);
                save(node);
                return;
            }
            final Node<U, V> childLeft = readChild(node, index);
            if (childLeft.pairs.size() >= treeFactor) {
                move(childLeft, node, childLeft.pairs.size() - 1, index);
            } else {
                final Node<U, V> childRight = readChild(node, index + 1);
                if (childRight.pairs.size() >= treeFactor) {
                    move(childRight, node, 0, index);
                } else {
                    childLeft.pairs.addAll(childRight.pairs);
                    node.pairs.remove(index);
                    getChildren(node).remove(index + 1);
                    delete(childRight);
                    if (node.pairs.isEmpty()) {
                        getVariables().root = childLeft;
                    }
                    save(childLeft);
                    save(node);
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
        final int index = getChildIndex(internalNode, key);
        final Node<U, V> child = readChild(internalNode, index);
        if (child.pairs.size() == treeFactor - 1) {
            Node<U, V> childLeft = null;
            Node<U, V> childRight = null;
            boolean moved = false;
            if (index > 0) {
                childLeft = readChild(internalNode, index - 1);
                if (childLeft.pairs.size() >= treeFactor) {
                    lend(internalNode, child, childLeft, index, true);
                    moved = true;
                }
            }
            if (!moved) {
                if (index < internalNode.children.size() - 1) {
                    childRight = readChild(internalNode, (index + 1));
                    if (childRight.pairs.size() >= treeFactor) {
                        lend(internalNode, child, childRight, index, false);
                        moved = true;
                    }
                }
            }
            if (!moved && childLeft != null && childLeft.pairs.size() == treeFactor - 1) {
                merge(internalNode, childLeft, child, index);
                moved = true;
            }
            if (!moved && childRight != null && childRight.pairs.size() == treeFactor - 1) {
                merge(internalNode, child, childRight, index + 1);
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

    private void merge(InternalNode<U, V> node, Node<U, V> childLeft, Node<U, V> childRight, int index) {
        node.children.remove(index);
        childLeft.pairs.add(node.pairs.remove(index - 1));
        save(node);
        childLeft.pairs.addAll(childRight.pairs);
        if (!isLeaf(childLeft)) {
            getChildren(childLeft).addAll(getChildren(childRight));
        }
        delete(childRight);
        if (node.pairs.isEmpty()) {
            getVariables().root = childLeft;
        }
        save(childLeft);
    }

    private Pair<Integer, Pair<U, Set<V>>> find(List<Pair<U, Set<V>>> pairs, U key, Function<Integer, Boolean> compareFunction) {
        for (int i = 0; i < pairs.size(); i++) {
            final Pair<U, Set<V>> pair = pairs.get(i);
            if (compareFunction.apply(key.compareTo(pair.getFirst()))) {
                return new Pair<>(i, pair);
            }
        }
        return null;
    }

    private void move(Node<U, V> nodeFrom, Node<U, V> nodeTo, int indexFrom, int indexTo) {
        final Pair<U, Set<V>> pairFrom = nodeFrom.pairs.get(indexFrom);
        delete(nodeFrom, pairFrom.getFirst());
        nodeTo.pairs.set(indexTo, pairFrom);
        save(nodeFrom);
        save(nodeTo);
    }

    private void lend(Node<U, V> node, Node<U, V> child, Node<U, V> donor, int index, boolean left) {
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
    }

    @Override
    public synchronized Set<V> searchNotNull(ConditionService conditionService, SimpleCondition condition) {
        return Collections.emptySet();
    }

    @Override
    public synchronized Set<V> searchNotNull(U key) {
        return search(getVariables().root, key).getSecond();
    }

    @Override
    public void clear() {
        final String searchPath;
        if (new File(path).isDirectory()) {
            searchPath = path;
        } else {
            searchPath = System.getProperty("user.dir");
        }
        for (File file : Objects.requireNonNull(new File(searchPath).listFiles((file, name) -> name.endsWith(fieldName)))) {
            file.delete();
        }
    }

    private synchronized Pair<Node<U, V>, Set<V>> search(Node<U, V> node, U key) {
        if (node == null || node.pairs.isEmpty()) {
            return new Pair<>(null, Collections.emptySet());
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
            return new Pair<>(node, pair.getSecond());
        } else if (isLeaf(node)) {
            return new Pair<>(node, Collections.emptySet());
        }
        return search(readChild(node, index), key);
    }

    protected synchronized Node<U, V> readChild(Node<U, V> node, int index) {
        if (isLeaf(node)) {
            return null;
        }
        return read(getChildren(node).get(index));
    }

    private synchronized Node<U, V> read(Node<U, V> node) {
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

    @Override
    public synchronized void destroy() {
        objectConverter.toFile(getVariables(), getFileName());
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
     * Для тестов
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
        public final List<Node<U, V>> children = new ArrayList<>();

        private InternalNode() {
            pairs = new ArrayList<>();
        }
    }

    public static class LeafNode<U, V> extends Node<U, V> implements Destroyable {
        private static final long serialVersionUID = -4761087948139648759L;
        public final String fileName;
        private volatile boolean initialized = false;

        private LeafNode(String fileName) {
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
