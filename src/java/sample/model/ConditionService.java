package sample.model;

import sample.model.pojo.ICondition;

public interface ConditionService {

    public ICondition parse(String input);

    public <T> boolean check(T value, ICondition condition);

}
