package server.model.impl;

import server.model.Buffer;
import server.model.pojo.TableType;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class BufferImpl<V extends TableType> implements Buffer<V> {
    private final Map<Integer, Element<V>> map = new ConcurrentHashMap<>();
    private final int maxSize;
    private final Consumer<Runnable> runnableConsumer;
    private final Consumer<List<Element<V>>> flushConsumer;

    public BufferImpl(int maxSize, Consumer<Runnable> runnableConsumer, Consumer<List<Element<V>>> flushConsumer) {
        this.maxSize = maxSize;
        this.runnableConsumer = runnableConsumer;
        this.flushConsumer = flushConsumer;
    }

    @Override
    public void add(V value, State state) {
        if (value == null) {
            return;
        }
        if (map.size() >= maxSize) {
            flush();
        }
        map.put(value.getId(), new Element<>(value, state));
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
    public void stream(Consumer<Element<V>> consumer) {
        sortedList().forEach(consumer);
    }

    @Override
    public void flush() {
        runnableConsumer.accept(() -> {
            if (flushConsumer != null) {
                flushConsumer.accept(sortedList());
            }
            map.clear();
        });
    }

    private List<Element<V>> sortedList() {
        return map.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey)).map(Map.Entry::getValue).collect(Collectors.toList());
    }
}
