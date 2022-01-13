package server.model.pojo;

import java.util.Set;

public interface ComplexCondition<T extends ICondition> extends ICondition {
    ComplexType getType();

    Set<T> getConditions();
}
