package sample.model;

import sample.model.lock.LockService;
import sample.model.lock.ReadWriteLock;
import sample.model.pojo.TableType;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class BufferImpl<V extends TableType> implements Buffer<V> {
    private final Map<Integer, Element<V>> map = new ConcurrentHashMap<>();
    private final ReadWriteLock<Object> readWriteLock = LockService.createReadWriteLock(Object.class);
    private final int maxSize;
    private final Consumer<List<Element<V>>> flushConsumer;

    public BufferImpl(int maxSize, Consumer<List<Element<V>>> flushConsumer) {
        this.maxSize = maxSize;
        this.flushConsumer = flushConsumer;
    }

    @Override
    public void add(V value, State state) {
        LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Read, ReadWriteLock.DEFAULT, () -> {
            if (value == null) {
                return;
            }
            if (map.size() == maxSize) {
                flush();
            }
            map.put(value.getId(), new Element<>(value, state));
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
    public void stream(Consumer<Element<V>> consumer) {
        sortedList().forEach(consumer);
    }

    @Override
    public void flush() {
        if (flushConsumer != null) {
            flushConsumer.accept(sortedList());
        }
        LockService.doInReadWriteLock(readWriteLock, LockService.LockType.Write, ReadWriteLock.DEFAULT, map::clear);
    }

    private List<Element<V>> sortedList() {
        return map.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey)).map(Map.Entry::getValue).collect(Collectors.toList());
    }
}
