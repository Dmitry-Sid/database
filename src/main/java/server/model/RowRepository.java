package server.model;

import server.model.pojo.ICondition;
import server.model.pojo.Row;

import java.util.List;

public interface RowRepository extends Repository<Row> {

    void add(Row row);

    List<Row> getList(ICondition iCondition, int from, int size);

}
