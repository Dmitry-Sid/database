package server.model.impl;

import server.model.BaseFieldKeeper;
import server.model.ConditionService;
import server.model.ObjectConverter;
import server.model.pojo.Pair;
import server.model.pojo.SimpleCondition;

import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class BTree<U extends Comparable<U>, V> extends BaseFieldKeeper<U, V> {
    public final int treeFactor;

    public BTree(String fieldName, String path, ObjectConverter objectConverter, int treeFactor) {
        super(fieldName, path, objectConverter);
        this.treeFactor = treeFactor;
    }

    @Override
    protected Variables<U, V> createVariables() {
        return new BTreeVariables<>(createNode(), new AtomicLong(Long.MIN_VALUE));
    }

    @Override
    public synchronized void insertNotNull(U key, V value) {
        if (getVariables().root.pairs.size() != treeFactor * 2 - 1) {
            insert(getVariables().root, key, value);
            return;
        }
        final Node<U, V> created = createNode();
        created.leaf = false;
        save(getVariables().root);
        created.children.add(getVariables().root.fileName);
        getVariables().root = created;
        split(created, 0);
        insert(created, key, value);
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
        if (node.leaf) {
            node.pairs.add(index, new Pair<>(key, new HashSet<>(Collections.singleton(value))));
            save(node);
            return;
        }
        final Node<U, V> child = read(node.children.get(index));
        if (child.pairs.size() == 2 * treeFactor - 1) {
            split(node, index);
            if (key.compareTo(node.pairs.get(index).getFirst()) > 0) {
                index++;
            }
        }
        insert(read(node.children.get(index)), key, value);
    }

    private int getChildIndex(Node<U, V> node, U key) {
        final Pair<Integer, Pair<U, Set<V>>> pair = find(node.pairs, key, compareResult -> compareResult < 0);
        if (pair != null) {
            return pair.getFirst();
        }
        return node.pairs.size();
    }

    public synchronized void split(Node<U, V> node, int index) {
        final Node<U, V> right = createNode();
        final Node<U, V> left = read(node.children.get(index));
        right.leaf = left.leaf;
        rearrange(left.pairs, right.pairs, treeFactor);
        if (!left.leaf) {
            rearrange(left.children, right.children, treeFactor);
        }
        node.pairs.add(index, left.pairs.get(left.pairs.size() - 1));
        left.pairs.remove(left.pairs.size() - 1);
        node.children.add(index + 1, right.fileName);
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
        if (!search(key).contains(value)) {
            return NOT;
        }
        return delete(getVariables().root, key, value, false);
    }

    private synchronized DeleteResult delete(Node<U, V> node, U key, V value, boolean fully) {
        final Pair<Integer, Pair<U, Set<V>>> pair = find(node.pairs, key, compareResult -> compareResult == 0);
        if (pair != null) {
            if (fully) {
                pair.getSecond().getSecond().clear();
            } else {
                pair.getSecond().getSecond().remove(value);
            }
            if (pair.getSecond().getSecond().isEmpty()) {
                if (node.leaf) {
                    node.pairs.remove(pair.getFirst().intValue());
                    save(node);
                    return FULLY;
                }
                final Node<U, V> childLeft = read(node.children.get(pair.getFirst()));
                if (childLeft.pairs.size() >= treeFactor) {
                    move(childLeft, node, childLeft.pairs.size() - 1, pair.getFirst());
                } else {
                    final Node<U, V> childRight = read(node.children.get(pair.getFirst() + 1));
                    if (childRight.pairs.size() >= treeFactor) {
                        move(childRight, node, 0, pair.getFirst());
                    } else {
                        childLeft.pairs.addAll(childRight.pairs);
                        save(childLeft);
                        node.pairs.remove(pair.getFirst().intValue());
                        node.children.remove(childRight.fileName);
                        save(node);
                        if (node.pairs.isEmpty()) {
                            getVariables().root = childLeft;
                        }
                    }
                }
                return FULLY;
            }
            save(node);
            return NOT_FULLY;
        }
        final int index = getChildIndex(node, key);
        if (node.children.size() == 0) {
            return NOT;
        }
        final Node<U, V> child = read(node.children.get(index));
        final DeleteResult deleteResult = delete(child, key, value, fully);
        if (deleteResult.fully && child.pairs.size() == treeFactor - 1) {
            Node<U, V> childLeft = null;
            Node<U, V> childRight = null;
            boolean moved = false;
            if (index > 0) {
                childLeft = read(node.children.get(index - 1));
                if (childLeft.pairs.size() >= treeFactor) {
                    lend(node, child, childLeft, index, true);
                    moved = true;
                }
            }
            if (!moved) {
                if (index < node.children.size() - 1) {
                    childRight = read(node.children.get(index + 1));
                    if (childRight.pairs.size() >= treeFactor) {
                        lend(node, child, childRight, index, false);
                        moved = true;
                    }
                }
            }
            if (!moved && childLeft != null && childRight != null && childLeft.pairs.size() == treeFactor - 1 && childRight.pairs.size() == treeFactor - 1) {
                node.children.remove(index);
                save(node);
                childLeft.pairs.add(node.pairs.remove(index - 1));
                childLeft.pairs.addAll(child.pairs);
                childLeft.children.addAll(child.children);
                save(childLeft);
                if (node.pairs.isEmpty()) {
                    getVariables().root = childLeft;
                }
            }
        }
        return deleteResult;
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
        delete(nodeFrom, pairFrom.getFirst(), null, true);
        nodeTo.pairs.set(indexTo, pairFrom);
        save(nodeFrom);
        save(nodeTo);
    }

    private void lend(Node<U, V> node, Node<U, V> child, Node<U, V> donor, int index, boolean left) {
        final Pair<U, Set<V>> nodePair = node.pairs.get(left ? index - 1 : index);
        final Pair<U, Set<V>> donorPair = donor.pairs.remove(left ? donor.pairs.size() - 1 : 0);
        child.pairs.add(left ? 0 : child.pairs.size(), nodePair);
        node.pairs.set(left ? index - 1 : index, donorPair);
        save(node);
        save(child);
        save(donor);
    }

    @Override
    public synchronized Set<V> searchNotNull(ConditionService conditionService, SimpleCondition condition) {
        return null;
    }

    @Override
    public synchronized Set<V> searchNotNull(U key) {
        return search(getVariables().root, key);
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

    private synchronized Set<V> search(Node<U, V> node, U key) {
        if (node == null || node.pairs.isEmpty()) {
            return Collections.emptySet();
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
            return pair.getSecond();
        } else if (node.leaf) {
            return Collections.emptySet();
        }
        return search(read(node.children.get(index)), key);
    }

    public synchronized Node<U, V> read(String fileName) {
        Node<U, V> node = getVariables().nodeMap.get(fileName);
        if (node == null) {
            node = objectConverter.fromFile(Node.class, fileName);
            node.init();
            save(node);
        }
        return node;
    }

    @Override
    public synchronized void destroy() {
        objectConverter.toFile(getVariables(), getFileName());
    }

    private Node<U, V> createNode() {
        final Node<U, V> node = new Node<>(getFileName(getVariables() == null ? Long.MIN_VALUE : getVariables().counter.incrementAndGet()));
        node.init();
        return node;
    }

    private void save(Node<U, V> node) {
        getVariables().nodeMap.put(node.fileName, node);
    }

    public static class BTreeVariables<U, V> extends Variables<U, V> {
        private static final long serialVersionUID = -4536650721479536430L;
        public volatile Node<U, V> root;
        private final AtomicLong counter;
        public final Map<String, Node<U, V>> nodeMap = new ConcurrentHashMap<>();

        private BTreeVariables(Node<U, V> root, AtomicLong counter) {
            this.root = root;
            this.counter = counter;
        }
    }

    public static class Node<U, V> implements Serializable {
        public static final long serialVersionUID = -1534435787722841767L;
        public final String fileName;
        public volatile boolean initialized = false;
        public volatile boolean leaf = true;
        public volatile List<Pair<U, Set<V>>> pairs;
        public volatile List<String> children;

        public Node(String fileName) {
            this.fileName = fileName;
        }

        public synchronized void init() {
            if (!initialized) {
                pairs = new ArrayList<>();
                children = new ArrayList<>();
                initialized = true;
            }
        }
    }

    private BTreeVariables<U, V> getVariables() {
        return (BTreeVariables<U, V>) variables;
    }
}
