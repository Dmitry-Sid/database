package sample.model;

import sample.model.pojo.ICondition;
import sample.model.pojo.Row;

import java.util.List;

public interface RowRepository extends Repository<Row> {

    public void add(Row row);

    public List<Row> getList(ICondition iCondition, int from, int size);

}
