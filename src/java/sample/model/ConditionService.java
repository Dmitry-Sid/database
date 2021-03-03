package sample.model;

import sample.model.pojo.ICondition;
import sample.model.pojo.Row;

public interface ConditionService {

    public ICondition parse(String input);

    public boolean check(Row row, ICondition condition);

    public ICondition getFieldCondition(ICondition condition, String field);
}
