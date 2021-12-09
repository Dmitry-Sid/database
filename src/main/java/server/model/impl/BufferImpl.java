package server.model.impl;

import server.model.Buffer;
import server.model.pojo.TableType;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class BufferImpl<V extends TableType> implements Buffer<V> {
    private final Map<Integer, Element<V>> map = new ConcurrentHashMap<>();
    private final int maxSize;
    private final Consumer<List<Element<V>>> flushConsumer;

    public BufferImpl(int maxSize, Consumer<List<Element<V>>> flushConsumer) {
        this.maxSize = maxSize;
        this.flushConsumer = flushConsumer;
    }

    @Override
    public void add(V value, State state) {
        if (value == null) {
            return;
        }
        map.compute(value.getId(), (k, v) -> {
            if (v == null) {
                return new Element<>(value, state);
            }
            final State finalState;
            if (!v.isFlushed() && State.DELETED != state) {
                finalState = State.ADDED;
            } else {
                finalState = state;
            }
            return new Element<>(value, finalState);
        });
    }

    @Override
    public Element<V> get(int id) {
        return map.get(id);
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public void stream(Consumer<Element<V>> consumer, AtomicBoolean stopChecker) {
        for (Element<V> element : sortedList()) {
            if (stopChecker != null && stopChecker.get()) {
                return;
            }
            consumer.accept(element);
        }
    }

    @Override
    public void flush() {
        final List<Element<V>> list = flushableList();
        if (flushConsumer != null) {
            flushConsumer.accept(list);
        }
        int mapSize = map.size();
        if (mapSize > maxSize) {
            for (Iterator<Map.Entry<Integer, Element<V>>> iterator = map.entrySet().iterator(); iterator.hasNext(); ) {
                final Map.Entry<Integer, Element<V>> entry = iterator.next();
                if (entry.getValue().isFlushed()) {
                    iterator.remove();
                    mapSize--;
                    if (mapSize <= maxSize) {
                        break;
                    }
                }
            }
        }
        list.forEach(element -> {
            if (State.DELETED.equals(element.getState())) {
                map.remove(element.getValue().getId());
            } else {
                element.setFlushed(true);
            }
        });
    }

    private List<Element<V>> sortedList() {
        return list(entry -> true);
    }

    private List<Element<V>> flushableList() {
        return list(entry -> !entry.getValue().isFlushed());
    }

    private List<Element<V>> list(Predicate<? super Map.Entry<Integer, Element<V>>> predicate) {
        return map.entrySet().stream().filter(predicate).sorted(Comparator.comparing(Map.Entry::getKey))
                .map(Map.Entry::getValue).collect(Collectors.toList());
    }
}
