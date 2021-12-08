package server.model;

import server.model.pojo.ICondition;

public interface ConditionService {

    ICondition parse(String input) throws ConditionException;

    <T> boolean check(T value, ICondition condition);

}
