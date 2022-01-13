package server.model.pojo;

import java.util.Objects;
import java.util.Set;

public abstract class AbstractComplexCondition<T extends ICondition> implements ComplexCondition<T> {
    protected final ComplexType type;
    protected final Set<T> conditions;

    protected AbstractComplexCondition(ComplexType type, Set<T> conditions) {
        this.type = type;
        this.conditions = conditions;
    }

    @Override
    public Set<T> getConditions() {
        return conditions;
    }

    @Override
    public ComplexType getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AbstractComplexCondition<?> that = (AbstractComplexCondition<?>) o;
        return Objects.equals(getConditions(), that.getConditions()) &&
                type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getConditions(), type);
    }
}