import org.junit.Test;
import sample.model.ConditionService;
import sample.model.ConditionServiceImpl;
import sample.model.pojo.ComplexCondition;
import sample.model.pojo.ICondition;
import sample.model.pojo.Row;
import sample.model.pojo.SimpleCondition;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class ConditionServiceTest {
    private static final ConditionService conditionService = new ConditionServiceImpl();

    @Test
    public void checkSimpleTest() {
        final Map<String, Comparable> map = new HashMap<>();
        map.put("int", 50);
        map.put("double", 25.15);
        map.put("String", "test");
        final Row row = new Row(0, map);
        {
            final ICondition condition = new SimpleCondition(ICondition.SimpleType.EQ, "int", 50);
            assertTrue(conditionService.check(row, condition));
        }
        {
            final ICondition condition = new SimpleCondition(ICondition.SimpleType.EQ, "int", 49);
            assertFalse(conditionService.check(row, condition));
        }
        {
            {
                final ICondition condition = new SimpleCondition(ICondition.SimpleType.GT, "int", 49);
                assertTrue(conditionService.check(row, condition));
            }
            {
                final ICondition condition = new SimpleCondition(ICondition.SimpleType.GT, "int", 50);
                assertFalse(conditionService.check(row, condition));
            }
            {
                final ICondition condition = new SimpleCondition(ICondition.SimpleType.GTE, "int", 49);
                assertTrue(conditionService.check(row, condition));
            }
            {
                final ICondition condition = new SimpleCondition(ICondition.SimpleType.GTE, "int", 50);
                assertTrue(conditionService.check(row, condition));
            }
            {
                final ICondition condition = new SimpleCondition(ICondition.SimpleType.GTE, "int", 51);
                assertFalse(conditionService.check(row, condition));
            }
        }
        {
            {
                final ICondition condition = new SimpleCondition(ICondition.SimpleType.LT, "int", 51);
                assertTrue(conditionService.check(row, condition));
            }
            {
                final ICondition condition = new SimpleCondition(ICondition.SimpleType.LT, "int", 49);
                assertFalse(conditionService.check(row, condition));
            }
            {
                final ICondition condition = new SimpleCondition(ICondition.SimpleType.LTE, "int", 51);
                assertTrue(conditionService.check(row, condition));
            }
            {
                final ICondition condition = new SimpleCondition(ICondition.SimpleType.LTE, "int", 50);
                assertTrue(conditionService.check(row, condition));
            }
            {
                final ICondition condition = new SimpleCondition(ICondition.SimpleType.LTE, "int", 49);
                assertFalse(conditionService.check(row, condition));
            }
        }
        {
            {
                final ICondition condition = new SimpleCondition(ICondition.SimpleType.LIKE, "String", "es");
                assertTrue(conditionService.check(row, condition));
            }
            {
                final ICondition condition = new SimpleCondition(ICondition.SimpleType.LIKE, "String", "fa");
                assertFalse(conditionService.check(row, condition));
            }
        }
    }

    @Test
    public void checkComplexTest() {
        final Map<String, Comparable> map = new HashMap<>();
        map.put("int", 50);
        map.put("double", 25.15);
        map.put("String", "test");
        final Row row = new Row(0, map);
        {
            final ICondition condition = new ComplexCondition(ICondition.ComplexType.AND,
                    new SimpleCondition(ICondition.SimpleType.EQ, "int", 50),
                    new SimpleCondition(ICondition.SimpleType.EQ, "double", 25.15));
            assertTrue(conditionService.check(row, condition));
        }
        {
            final ICondition condition = new ComplexCondition(ICondition.ComplexType.AND,
                    new SimpleCondition(ICondition.SimpleType.EQ, "int", 50),
                    new SimpleCondition(ICondition.SimpleType.EQ, "double", 25.16));
            assertFalse(conditionService.check(row, condition));
        }
        {
            final ICondition condition = new ComplexCondition(ICondition.ComplexType.AND,
                    new SimpleCondition(ICondition.SimpleType.EQ, "int", 50),
                    new SimpleCondition(ICondition.SimpleType.EQ, "double", 25.15),
                    new SimpleCondition(ICondition.SimpleType.EQ, "String", "test"));
            assertTrue(conditionService.check(row, condition));
        }
        {
            final ICondition condition = new ComplexCondition(ICondition.ComplexType.AND,
                    new SimpleCondition(ICondition.SimpleType.EQ, "int", 50),
                    new SimpleCondition(ICondition.SimpleType.EQ, "double", 25.15),
                    new SimpleCondition(ICondition.SimpleType.EQ, "String", "tes"));
            assertFalse(conditionService.check(row, condition));
        }
        {
            final ICondition condition = new ComplexCondition(ICondition.ComplexType.OR,
                    new SimpleCondition(ICondition.SimpleType.EQ, "int", 50),
                    new SimpleCondition(ICondition.SimpleType.EQ, "double", 30.0),
                    new SimpleCondition(ICondition.SimpleType.EQ, "String", "tes"));
            assertTrue(conditionService.check(row, condition));
        }
        {
            final ICondition condition = new ComplexCondition(ICondition.ComplexType.OR,
                    new ComplexCondition(ICondition.ComplexType.AND,
                            new SimpleCondition(ICondition.SimpleType.EQ, "int", 50),
                            new SimpleCondition(ICondition.SimpleType.EQ, "double", 30.0)),
                    new SimpleCondition(ICondition.SimpleType.EQ, "String", "test"));
            assertTrue(conditionService.check(row, condition));
        }
        {
            final ICondition condition = new ComplexCondition(ICondition.ComplexType.OR,
                    new ComplexCondition(ICondition.ComplexType.AND,
                            new SimpleCondition(ICondition.SimpleType.EQ, "int", 50),
                            new SimpleCondition(ICondition.SimpleType.EQ, "double", 30.0)),
                    new ComplexCondition(ICondition.ComplexType.AND,
                            new SimpleCondition(ICondition.SimpleType.LTE, "int", 50),
                            new SimpleCondition(ICondition.SimpleType.EQ, "String", "test")));
            assertTrue(conditionService.check(row, condition));
        }
        {
            final ICondition condition = new ComplexCondition(ICondition.ComplexType.OR,
                    new ComplexCondition(ICondition.ComplexType.AND,
                            new SimpleCondition(ICondition.SimpleType.EQ, "int", 50),
                            new SimpleCondition(ICondition.SimpleType.EQ, "double", 30.0)),
                    new ComplexCondition(ICondition.ComplexType.AND,
                            new SimpleCondition(ICondition.SimpleType.LTE, "int", 49),
                            new SimpleCondition(ICondition.SimpleType.EQ, "String", "test")));
            assertFalse(conditionService.check(row, condition));
        }
        {
            final ICondition condition = new ComplexCondition(ICondition.ComplexType.OR,
                    new ComplexCondition(ICondition.ComplexType.AND,
                            new SimpleCondition(ICondition.SimpleType.EQ, "int", 50),
                            new SimpleCondition(ICondition.SimpleType.EQ, "double", 30.0)),
                    new ComplexCondition(ICondition.ComplexType.AND,
                            new SimpleCondition(ICondition.SimpleType.LTE, "int", 50),
                            new SimpleCondition(ICondition.SimpleType.EQ, "String", "tes")));
            assertFalse(conditionService.check(row, condition));
        }
    }

}
