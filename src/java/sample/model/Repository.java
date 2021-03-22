package sample.model;

import sample.model.pojo.TableType;

import java.util.function.Consumer;

public interface Repository<T extends TableType> {

    public void delete(int id);

    public boolean process(int id, Consumer<T> consumer);

}
