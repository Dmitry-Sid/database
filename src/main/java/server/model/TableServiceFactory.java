package server.model;

import java.util.Map;
import java.util.function.Consumer;

public interface TableServiceFactory {
    void createServices(String tableName, Consumer<Map<Class<?>, Object>> consumer, Class<?>... classes);
}
