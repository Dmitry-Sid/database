package sample.model;

import sample.model.pojo.BinarySearchDirection;
import sample.model.pojo.ICondition;
import sample.model.pojo.SimpleCondition;

public interface ConditionService {

    public ICondition parse(String input);

    public <T> boolean check(T value, ICondition condition);

    public BinarySearchDirection determineDirection(Comparable value, SimpleCondition condition);
}
