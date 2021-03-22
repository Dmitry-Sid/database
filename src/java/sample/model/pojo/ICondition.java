package sample.model.pojo;

public interface ICondition {
    public static final EmptyCondition empty = new EmptyCondition();

    public enum SimpleType {
        EQ, LT, LTE, GT, GTE, LIKE, NOT
    }

    public enum ComplexType {
        AND, OR
    }
}
