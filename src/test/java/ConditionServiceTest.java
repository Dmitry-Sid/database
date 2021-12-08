import org.junit.Test;
import server.model.ConditionException;
import server.model.ConditionService;
import server.model.impl.ConditionServiceImpl;
import server.model.pojo.ComplexCondition;
import server.model.pojo.ICondition;
import server.model.pojo.Row;
import server.model.pojo.SimpleCondition;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.*;
import static org.junit.Assert.assertNotEquals;

public class ConditionServiceTest {
    private static final ConditionService conditionService = new ConditionServiceImpl(TestUtils.mockModelService());

    @Test
    public void checkSimpleTest() throws ConditionException {
        final Map<String, Comparable> map = new HashMap<>();
        map.put("int", 50);
        map.put("double", 25.15);
        map.put("String", "test");
        final Row row = new Row(0, map);
        {
            final ICondition condition = SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50);
            assertTrue(conditionService.check(row, condition));
            assertTrue(conditionService.check(row.getFields().get("int"), condition));
        }
        {
            final ICondition condition = SimpleCondition.make(ICondition.SimpleType.EQ, "int", 49);
            assertFalse(conditionService.check(row, condition));
            assertFalse(conditionService.check(row.getFields().get("int"), condition));
        }
        {
            final ICondition condition = SimpleCondition.make(ICondition.SimpleType.EQ, "int", null);
            assertFalse(conditionService.check(row, condition));
            assertFalse(conditionService.check(row.getFields().get("int"), condition));
        }
        {
            final ICondition condition = SimpleCondition.make(ICondition.SimpleType.NOT, "int", null);
            assertTrue(conditionService.check(row, condition));
            assertTrue(conditionService.check(row.getFields().get("int"), condition));
        }
        {
            final ICondition condition = SimpleCondition.make(ICondition.SimpleType.EQ, "int1", null);
            assertTrue(conditionService.check(row, condition));
            assertTrue(conditionService.check(row.getFields().get("int1"), condition));
        }
        {
            final ICondition condition = SimpleCondition.make(ICondition.SimpleType.NOT, "int1", null);
            assertFalse(conditionService.check(row, condition));
            assertFalse(conditionService.check(row.getFields().get("int1"), condition));
        }
        testWrongConditions(row);
        {
            {
                final ICondition condition = SimpleCondition.make(ICondition.SimpleType.GT, "int", 49);
                assertTrue(conditionService.check(row, condition));
                assertTrue(conditionService.check(row.getFields().get("int"), condition));
            }
            {
                final ICondition condition = SimpleCondition.make(ICondition.SimpleType.GT, "int", 50);
                assertFalse(conditionService.check(row, condition));
                assertFalse(conditionService.check(row.getFields().get("int"), condition));
            }
            {
                final ICondition condition = SimpleCondition.make(ICondition.SimpleType.GTE, "int", 49);
                assertTrue(conditionService.check(row, condition));
                assertTrue(conditionService.check(row.getFields().get("int"), condition));
            }
            {
                final ICondition condition = SimpleCondition.make(ICondition.SimpleType.GTE, "int", 50);
                assertTrue(conditionService.check(row, condition));
                assertTrue(conditionService.check(row.getFields().get("int"), condition));
            }
            {
                final ICondition condition = SimpleCondition.make(ICondition.SimpleType.GTE, "int", 51);
                assertFalse(conditionService.check(row, condition));
                assertFalse(conditionService.check(row.getFields().get("int"), condition));
            }
        }
        {
            {
                final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LT, "int", 51);
                assertTrue(conditionService.check(row, condition));
                assertTrue(conditionService.check(row.getFields().get("int"), condition));
            }
            {
                final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LT, "int", 49);
                assertFalse(conditionService.check(row, condition));
                assertFalse(conditionService.check(row.getFields().get("int"), condition));
            }
            {
                final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LTE, "int", 51);
                assertTrue(conditionService.check(row, condition));
                assertTrue(conditionService.check(row.getFields().get("int"), condition));
            }
            {
                final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LTE, "int", 50);
                assertTrue(conditionService.check(row, condition));
                assertTrue(conditionService.check(row.getFields().get("int"), condition));
            }
            {
                final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LTE, "int", 49);
                assertFalse(conditionService.check(row, condition));
                assertFalse(conditionService.check(row.getFields().get("int"), condition));
            }
            {
                final ICondition condition = SimpleCondition.make(ICondition.SimpleType.NOT, "int", 49);
                assertTrue(conditionService.check(row, condition));
                assertTrue(conditionService.check(row.getFields().get("int"), condition));
            }
            {
                final ICondition condition = SimpleCondition.make(ICondition.SimpleType.NOT, "int", 50);
                assertFalse(conditionService.check(row, condition));
                assertFalse(conditionService.check(row.getFields().get("int"), condition));
            }
        }
        {
            {
                final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LIKE, "String", "es");
                assertTrue(conditionService.check(row, condition));
                assertTrue(conditionService.check(row.getFields().get("String"), condition));
            }
            {
                final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LIKE, "String", "fa");
                assertFalse(conditionService.check(row, condition));
                assertFalse(conditionService.check(row.getFields().get("String"), condition));
            }
        }
    }

    private void testWrongConditions(Row row) {
        testWrongCondition(row, ICondition.SimpleType.LT);
        testWrongCondition(row, ICondition.SimpleType.LTE);
        testWrongCondition(row, ICondition.SimpleType.GT);
        testWrongCondition(row, ICondition.SimpleType.GTE);
        testWrongCondition(row, ICondition.SimpleType.LIKE);
    }

    private void testWrongCondition(Row row, ICondition.SimpleType simpleType) {
        String exception = null;
        try {
            final ICondition condition = SimpleCondition.make(simpleType, "int", null);
            assertFalse(conditionService.check(row, condition));
            assertFalse(conditionService.check(row.getFields().get("int"), condition));
        } catch (Exception e) {
            exception = e.getMessage();
        }
        assertEquals("wrong condition int, null values allowed only for EQ and NOT", exception);
    }

    @Test
    public void checkComplexTest() throws ConditionException {
        final Map<String, Comparable> map = new HashMap<>();
        map.put("int", 50);
        map.put("double", 25.15);
        map.put("String", "test");
        final Row row = new Row(0, map);
        {
            final ICondition condition = ComplexCondition.make(ICondition.ComplexType.AND,
                    SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "double", 25.15));
            assertTrue(conditionService.check(row, condition));
        }
        {
            final ICondition condition = ComplexCondition.make(ICondition.ComplexType.AND,
                    SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "double", 25.16));
            assertFalse(conditionService.check(row, condition));
        }
        {
            final ICondition condition = ComplexCondition.make(ICondition.ComplexType.AND,
                    SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "double", 25.15),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "String", "test"));
            assertTrue(conditionService.check(row, condition));
        }
        {
            final ICondition condition = ComplexCondition.make(ICondition.ComplexType.AND,
                    SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "double", 25.15),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "String", "tes"));
            assertFalse(conditionService.check(row, condition));
        }
        {
            final ICondition condition = ComplexCondition.make(ICondition.ComplexType.OR,
                    SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "double", 30.0),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "String", "tes"));
            assertTrue(conditionService.check(row, condition));
        }
        {
            final ICondition condition = ComplexCondition.make(ICondition.ComplexType.OR,
                    ComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "double", 30.0)),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "String", "test"));
            assertTrue(conditionService.check(row, condition));
        }
        {
            final ICondition condition = ComplexCondition.make(ICondition.ComplexType.OR,
                    ComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "double", 30.0)),
                    ComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.LTE, "int", 50),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "String", "test")));
            assertTrue(conditionService.check(row, condition));
        }
        {
            final ICondition condition = ComplexCondition.make(ICondition.ComplexType.OR,
                    ComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "double", 30.0)),
                    ComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.LTE, "int", 49),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "String", "test")));
            assertFalse(conditionService.check(row, condition));
        }
        {
            final ICondition condition = ComplexCondition.make(ICondition.ComplexType.OR,
                    ComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "double", 30.0)),
                    ComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.LTE, "int", 50),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "String", "tes")));
            assertFalse(conditionService.check(row, condition));
        }
        {
            final ICondition condition = ComplexCondition.make(ICondition.ComplexType.AND,
                    SimpleCondition.make(ICondition.SimpleType.GT, "int", 50),
                    SimpleCondition.make(ICondition.SimpleType.LT, "int", 100));
            assertTrue(conditionService.check(75, condition));
        }
        {
            final ICondition condition = ComplexCondition.make(ICondition.ComplexType.AND,
                    SimpleCondition.make(ICondition.SimpleType.GT, "int", 100),
                    SimpleCondition.make(ICondition.SimpleType.LT, "int", 50));
            assertFalse(conditionService.check(75, condition));
        }
    }

    @Test
    public void parseTest() throws ConditionException {
        assertEquals(SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50), conditionService.parse("   int  EQ         50 "));
        assertEquals(SimpleCondition.make(ICondition.SimpleType.EQ, "double", 22.3), conditionService.parse("double EQ 22.3"));
        assertNotEquals(SimpleCondition.make(ICondition.SimpleType.NOT, "int", 50), conditionService.parse("int EQ 50"));
        assertEquals(SimpleCondition.make(ICondition.SimpleType.NOT, "int", 50), conditionService.parse("int NOT 50"));
        assertEquals(SimpleCondition.make(ICondition.SimpleType.GTE, "int", 50), conditionService.parse("int GTE 50"));
        assertEquals(SimpleCondition.make(ICondition.SimpleType.GT, "int", 50), conditionService.parse("int GT 50"));
        assertEquals(SimpleCondition.make(ICondition.SimpleType.LTE, "int", 50), conditionService.parse("int LTE 50"));
        assertEquals(SimpleCondition.make(ICondition.SimpleType.LT, "int", 50), conditionService.parse("int LT 50"));
        assertEquals(SimpleCondition.make(ICondition.SimpleType.LIKE, "String", "se"), conditionService.parse("String LIKE se"));
        assertEquals(SimpleCondition.make(ICondition.SimpleType.EQ, "int", null), conditionService.parse("int EQ null"));
        assertEquals(SimpleCondition.make(ICondition.SimpleType.NOT, "String", null), conditionService.parse("String NOT null"));
        assertNotEquals(SimpleCondition.make(ICondition.SimpleType.EQ, "int", null), conditionService.parse("String NOT null"));
        {
            final ICondition condition = ComplexCondition.make(ICondition.ComplexType.OR,
                    SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "double", 30.0),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "String", "tes"));
            assertEquals(condition, conditionService.parse("OR(int EQ 50;double EQ 30.0;String EQ tes)"));
        }
        {
            final ICondition condition = ComplexCondition.make(ICondition.ComplexType.OR,
                    ComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "double", 30.0)),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "String", "test"),
                    ComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.LTE, "int", 50),
                            SimpleCondition.make(ICondition.SimpleType.GT, "double", 30.0)));
            assertEquals(condition, conditionService.parse("OR(AND(int EQ 50;double EQ 30.0);String EQ test;AND(int LTE 50;double GT 30.0))"));
        }
        {
            final ICondition condition = ComplexCondition.make(ICondition.ComplexType.OR,
                    ComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "double", 30.0)),
                    ComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.LTE, "int", 49),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "String", "test")));
            assertEquals(condition, conditionService.parse("OR(AND(int EQ 50;double EQ 30.0);AND(int LTE 49;String EQ test))"));
        }
        {
            final ICondition condition = ComplexCondition.make(ICondition.ComplexType.OR,
                    ComplexCondition.make(ICondition.ComplexType.AND,
                            ComplexCondition.make(ICondition.ComplexType.OR,
                                    SimpleCondition.make(ICondition.SimpleType.LIKE, "String", "te"),
                                    SimpleCondition.make(ICondition.SimpleType.NOT, "double", 30.0)),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "double", 30.0)),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "String", "test"),
                    ComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.LTE, "int", 50),
                            SimpleCondition.make(ICondition.SimpleType.GT, "double", 30.0),
                            ComplexCondition.make(ICondition.ComplexType.AND,
                                    SimpleCondition.make(ICondition.SimpleType.GT, "String", "st"),
                                    SimpleCondition.make(ICondition.SimpleType.GTE, "double", 20.0))));
            assertEquals(condition, conditionService.parse("OR(" +
                    "AND(OR(String LIKE te;double NOT 30.0);int EQ 50;double EQ 30.0);" +
                    "String EQ test;" +
                    "AND(int LTE 50;double GT 30.0;AND(String GT st;double GTE 20.0)))"));
        }
    }

}
