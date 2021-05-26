package server.model;

public interface Destroyable {

    void destroy();

    default void stop() {

    }
}
