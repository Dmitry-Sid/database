package server.model.impl;

import server.model.ConditionService;
import server.model.FieldKeeper;
import server.model.ObjectConverter;
import server.model.pojo.Pair;
import server.model.pojo.SimpleCondition;

import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class BTree<U extends Comparable<U>, V> extends FieldKeeper<U, V> {
    public final Variables variables;
    public final int treeFactor;

    public BTree(String fieldName, String fileName, ObjectConverter objectConverter, int treeFactor) {
        super(fieldName, fileName, objectConverter);
        this.treeFactor = treeFactor;
        if (new File(fileName).exists()) {
            this.variables = objectConverter.fromFile(Variables.class, fileName);
        } else {
            this.variables = new Variables(createNode("root"), new AtomicLong(Long.MIN_VALUE));
        }
        this.variables.nodeMap.put(this.variables.root.fileName, this.variables.root);
    }

    @Override
    public synchronized void insert(U key, V value) {
        if (variables.root.pairs.size() != treeFactor * 2 - 1) {
            insert(variables.root, key, value);
            return;
        }
        final Node<U, V> created = createNode("root");
        created.leaf = false;
        variables.root.fileName = generateFileName();
        variables.nodeMap.put(variables.root.fileName, variables.root);
        created.children.add(variables.root.fileName);
        variables.root = created;
        split(created, 0);
        insert(created, key, value);
    }

    private void insert(Node<U, V> node, U key, V value) {
        int index = node.pairs.size() - 1;
        for (; index >= 0; index--) {
            final Pair<U, Set<V>> pair = node.pairs.get(index);
            final int compareResult = key.compareTo(pair.getFirst());
            if (compareResult == 0) {
                pair.getSecond().add(value);
                return;
            } else if (compareResult > 0) {
                break;
            }
        }
        index++;
        if (node.leaf) {
            node.pairs.add(index, new Pair<>(key, new HashSet<>(Collections.singleton(value))));
            variables.nodeMap.put(node.fileName, node);
            return;
        }
        final Node<U, V> readNode = read(node.children.get(index));
        if (readNode.pairs.size() == 2 * treeFactor - 1) {
            split(node, index);
            if (key.compareTo(node.pairs.get(index).getFirst()) > 0) {
                index++;
            }
        }
        insert(read(node.children.get(index)), key, value);
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
        variables.nodeMap.put(node.fileName, node);
        variables.nodeMap.put(left.fileName, left);
        variables.nodeMap.put(right.fileName, right);
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
    public synchronized boolean delete(U key, V value) {
        return false;
    }

    @Override
    public synchronized Set<V> search(ConditionService conditionService, SimpleCondition condition) {
        return null;
    }

    @Override
    public synchronized Set<V> search(U key) {
        if (key == null) {
            return Collections.emptySet();
        }
        return search(variables.root, key);
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
        Node<U, V> node = variables.nodeMap.get(fileName);
        if (node == null) {
            node = objectConverter.fromFile(Node.class, fileName);
            node.init();
            variables.nodeMap.put(fileName, node);
        }
        return node;
    }

    @Override
    public synchronized void destroy() {

    }

    private Node<U, V> createNode() {
        return createNode(generateFileName());
    }

    private Node<U, V> createNode(String fileName) {
        final Node<U, V> node = new Node<>(fileName);
        node.init();
        return node;
    }

    private String generateFileName() {
        return fileName + variables.counter.incrementAndGet() + ".node";
    }

    public class Variables implements Serializable {
        private static final long serialVersionUID = -4536650721479536430L;
        public volatile Node<U, V> root;
        private final AtomicLong counter;
        public final Map<String, Node<U, V>> nodeMap = new ConcurrentHashMap<>();

        private Variables(Node<U, V> root, AtomicLong counter) {
            this.root = root;
            this.counter = counter;
        }
    }

    public static class Node<U, V> implements Serializable {
        public static final long serialVersionUID = -1534435787722841767L;
        public volatile String fileName;
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
}
