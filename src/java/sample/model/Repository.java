package sample.model;

import sample.model.pojo.ICondition;
import sample.model.pojo.Row;

import java.util.List;

public interface Repository {

    public void add(Row row);

    public void delete(int id);

    public Row get(int id);

    public List<Row> getList(ICondition iCondition, int from, int size);
}
