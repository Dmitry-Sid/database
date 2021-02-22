package sample.model;

import sample.model.pojo.Row;

public interface Repository {

    public void add(Row row);

    public void delete(String id);

    public Row get(int id);
}
