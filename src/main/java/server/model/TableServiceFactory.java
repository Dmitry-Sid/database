package server.model;

public interface TableServiceFactory {
    <T> T getService(String tableName, Class<T> clazz);

    void createServices(String tableName);
}
