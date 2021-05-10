package server.model;

import server.model.pojo.TableType;

import java.util.function.Consumer;

public interface Repository<T extends TableType> extends Destroyable {

    void delete(int id);

    boolean process(int id, Consumer<T> consumer);

}
