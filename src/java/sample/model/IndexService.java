package sample.model;

import sample.model.pojo.ICondition;

import java.util.Set;

public interface IndexService {

    public Set<Integer> search(ICondition iCondition);

}
