package server.model.pojo;

public interface ICondition {
    EmptyCondition empty = new EmptyCondition();

    enum SimpleType {
        EQ, LT, LTE, GT, GTE, LIKE, NOT
    }

    enum ComplexType {
        AND, OR
    }
}
