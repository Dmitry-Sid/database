package sample.model.pojo;

public interface ICondition {
    public enum SimpleType {
        EQ, LT, LTE, GT, GTE, LIKE
    }

    public enum ComplexType {
        AND, OR
    }
}
