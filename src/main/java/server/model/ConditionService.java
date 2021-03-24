package server.model;

import server.model.pojo.ICondition;

public interface ConditionService {

    public ICondition parse(String input);

    public <T> boolean check(T value, ICondition condition);

}
