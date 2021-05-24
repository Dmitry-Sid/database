package server.model;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class Utils {

    public static <V> boolean isFull(Collection<V> collection, int size) {
        return size >= 0 && collection.size() >= size;
    }

    public static <V> boolean fillToFull(Collection<V> mainCollection, int size, Collection<V> collection) {
        if (isFull(mainCollection, size)) {
            final List<V> reducedList = mainCollection.stream().limit(size).collect(Collectors.toList());
            mainCollection.clear();
            mainCollection.addAll(reducedList);
            return true;
        }
        for (V value : collection) {
            if (isFull(mainCollection, size)) {
                return true;
            }
            mainCollection.add(value);
        }
        return false;
    }

}
