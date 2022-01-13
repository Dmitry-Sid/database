import org.junit.Test;
import server.model.ConditionException;
import server.model.ConditionService;
import server.model.impl.ConditionServiceImpl;
import server.model.pojo.*;

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
            final ICondition condition = MultiComplexCondition.make(ICondition.ComplexType.AND,
                    SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "double", 25.15));
            assertTrue(conditionService.check(row, condition));
        }
        {
            final ICondition condition = MultiComplexCondition.make(ICondition.ComplexType.AND,
                    SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "double", 25.16));
            assertFalse(conditionService.check(row, condition));
        }
        {
            final ICondition condition = MultiComplexCondition.make(ICondition.ComplexType.AND,
                    SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "double", 25.15),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "String", "test"));
            assertTrue(conditionService.check(row, condition));
        }
        {
            final ICondition condition = MultiComplexCondition.make(ICondition.ComplexType.AND,
                    SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "double", 25.15),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "String", "tes"));
            assertFalse(conditionService.check(row, condition));
        }
        {
            final ICondition condition = MultiComplexCondition.make(ICondition.ComplexType.OR,
                    SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "double", 30.0),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "String", "tes"));
            assertTrue(conditionService.check(row, condition));
        }
        {
            final ICondition condition = MultiComplexCondition.make(ICondition.ComplexType.OR,
                    MultiComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "double", 30.0)),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "String", "test"));
            assertTrue(conditionService.check(row, condition));
        }
        {
            final ICondition condition = MultiComplexCondition.make(ICondition.ComplexType.OR,
                    MultiComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "double", 30.0)),
                    MultiComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.LTE, "int", 50),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "String", "test")));
            assertTrue(conditionService.check(row, condition));
        }
        {
            final ICondition condition = MultiComplexCondition.make(ICondition.ComplexType.OR,
                    MultiComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "double", 30.0)),
                    MultiComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.LTE, "int", 49),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "String", "test")));
            assertFalse(conditionService.check(row, condition));
        }
        {
            final ICondition condition = MultiComplexCondition.make(ICondition.ComplexType.OR,
                    MultiComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "double", 30.0)),
                    MultiComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.LTE, "int", 50),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "String", "tes")));
            assertFalse(conditionService.check(row, condition));
        }
        {
            final ICondition condition = MultiComplexCondition.make(ICondition.ComplexType.AND,
                    SimpleCondition.make(ICondition.SimpleType.GT, "int", 50),
                    SimpleCondition.make(ICondition.SimpleType.LT, "int", 100));
            assertTrue(conditionService.check(75, condition));
        }
        {
            final ICondition condition = MultiComplexCondition.make(ICondition.ComplexType.AND,
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
            final ICondition condition = MultiComplexCondition.make(ICondition.ComplexType.OR,
                    SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "double", 30.0),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "String", "tes"));
            assertEquals(condition, conditionService.parse("OR(int EQ 50;double EQ 30.0;String EQ tes)"));
        }
        {
            final ICondition condition = MultiComplexCondition.make(ICondition.ComplexType.OR,
                    SimpleCondition.make(ICondition.SimpleType.EQ, "String", "test"),
                    MultiComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "double", 30.0)),
                    MultiComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.LTE, "int", 50),
                            SimpleCondition.make(ICondition.SimpleType.GT, "double", 30.0)));
            assertEquals(condition, conditionService.parse("OR(AND(int EQ 50;double EQ 30.0);String EQ test;AND(int LTE 50;double GT 30.0))"));
        }
        {
            final ICondition condition = MultiComplexCondition.make(ICondition.ComplexType.OR,
                    MultiComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "double", 30.0)),
                    MultiComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.LTE, "int", 49),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "String", "test")));
            assertEquals(condition, conditionService.parse("OR(AND(int EQ 50;double EQ 30.0);AND(int LTE 49;String EQ test))"));
        }
        {
            final ICondition condition = MultiComplexCondition.make(ICondition.ComplexType.OR,
                    MultiComplexCondition.make(ICondition.ComplexType.AND,
                            MultiComplexCondition.make(ICondition.ComplexType.OR,
                                    SimpleCondition.make(ICondition.SimpleType.LIKE, "String", "te"),
                                    SimpleCondition.make(ICondition.SimpleType.NOT, "double", 30.0)),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50),
                            SimpleCondition.make(ICondition.SimpleType.EQ, "double", 30.0)),
                    SimpleCondition.make(ICondition.SimpleType.EQ, "String", "test"),
                    MultiComplexCondition.make(ICondition.ComplexType.AND,
                            SimpleCondition.make(ICondition.SimpleType.LTE, "int", 50),
                            SimpleCondition.make(ICondition.SimpleType.GT, "String", "st"),
                            SimpleCondition.make(ICondition.SimpleType.GT, "double", 30.0)));
            assertEquals(condition, conditionService.parse("OR(" +
                    "AND(OR(String LIKE te;double NOT 30.0);int EQ 50;double EQ 30.0);" +
                    "String EQ test;" +
                    "AND(int LTE 50;double GT 30.0;AND(String GT st;double GTE 20.0)))"));
        }
    }

    @Test
    public void transformTest() throws ConditionException {
        // AND
        {
            // EQ
            {
                // EQ
                {
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.EQ, "int", 20);
                        assertEquals(condition, conditionService.parse("AND(int EQ 20;int EQ 20)"));
                    }
                    {
                        try {
                            conditionService.parse("AND(int EQ 20;int EQ 50)");
                            fail();
                        } catch (ConditionException e) {
                            assertTrue("cannot be more than one EQ condition inside AND condition, conditionFirst SimpleCondition{type=EQ, field='int', value=50}, conditionSecond SimpleCondition{type=EQ, field='int', value=20}".equals(e.getMessage()) ||
                                    "cannot be more than one EQ condition inside AND condition, conditionFirst SimpleCondition{type=EQ, field='int', value=20}, conditionSecond SimpleCondition{type=EQ, field='int', value=50}".equals(e.getMessage()));
                        }
                    }
                }
                // LT
                {
                    {
                        try {
                            conditionService.parse("AND(int LT 20;int EQ 50)");
                            fail();
                        } catch (ConditionException e) {
                            assertTrue("EQ condition cannot have greater or equal value than LT value, conditionFirst SimpleCondition{type=EQ, field='int', value=50}, conditionSecond SimpleCondition{type=LT, field='int', value=20}".equals(e.getMessage()) ||
                                    "LT condition cannot have lower or equal value than EQ value, conditionFirst SimpleCondition{type=LT, field='int', value=20}, conditionSecond SimpleCondition{type=EQ, field='int', value=50}".equals(e.getMessage()));
                        }
                    }
                    {
                        try {
                            conditionService.parse("AND(int LT 20;int EQ 20)");
                            fail();
                        } catch (ConditionException e) {
                            assertTrue("EQ condition cannot have greater or equal value than LT value, conditionFirst SimpleCondition{type=EQ, field='int', value=20}, conditionSecond SimpleCondition{type=LT, field='int', value=20}".equals(e.getMessage()) ||
                                    "LT condition cannot have lower or equal value than EQ value, conditionFirst SimpleCondition{type=LT, field='int', value=20}, conditionSecond SimpleCondition{type=EQ, field='int', value=20}".equals(e.getMessage()));
                        }
                    }
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.EQ, "int", 10);
                        assertEquals(condition, conditionService.parse("AND(int LT 20;int EQ 10)"));
                    }
                }
                // LTE
                {
                    {
                        try {
                            conditionService.parse("AND(int LTE 20;int EQ 50)");
                            fail();
                        } catch (ConditionException e) {
                            assertTrue("EQ condition cannot have greater value than LTE value, conditionFirst SimpleCondition{type=EQ, field='int', value=50}, conditionSecond SimpleCondition{type=LTE, field='int', value=20}".equals(e.getMessage()) ||
                                    "LTE condition cannot have lower value than EQ value, conditionFirst SimpleCondition{type=LTE, field='int', value=20}, conditionSecond SimpleCondition{type=EQ, field='int', value=50}".equals(e.getMessage()));
                        }
                    }
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.EQ, "int", 20);
                        assertEquals(condition, conditionService.parse("AND(int LTE 20;int EQ 20)"));
                    }
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.EQ, "int", 10);
                        assertEquals(condition, conditionService.parse("AND(int LTE 20;int EQ 10)"));
                    }
                }
                // GT
                {
                    {
                        try {
                            conditionService.parse("AND(int GT 80;int EQ 50)");
                            fail();
                        } catch (ConditionException e) {
                            assertTrue("EQ condition cannot have lower or equal value than GT value, conditionFirst SimpleCondition{type=EQ, field='int', value=50}, conditionSecond SimpleCondition{type=GT, field='int', value=80}".equals(e.getMessage()) ||
                                    "GT condition cannot have greater or equal value than EQ value, conditionFirst SimpleCondition{type=GT, field='int', value=80}, conditionSecond SimpleCondition{type=EQ, field='int', value=50}".equals(e.getMessage()));
                        }
                    }
                    {
                        try {
                            conditionService.parse("AND(int GT 80;int EQ 80)");
                            fail();
                        } catch (ConditionException e) {
                            assertTrue("EQ condition cannot have lower or equal value than GT value, conditionFirst SimpleCondition{type=EQ, field='int', value=80}, conditionSecond SimpleCondition{type=GT, field='int', value=80}".equals(e.getMessage()) ||
                                    "GT condition cannot have greater or equal value than EQ value, conditionFirst SimpleCondition{type=GT, field='int', value=80}, conditionSecond SimpleCondition{type=EQ, field='int', value=80}".equals(e.getMessage()));
                        }
                    }
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.EQ, "int", 90);
                        assertEquals(condition, conditionService.parse("AND(int GT 80;int EQ 90)"));
                    }
                }
                // GTE
                {
                    {
                        try {
                            conditionService.parse("AND(int GTE 80;int EQ 50)");
                            fail();
                        } catch (ConditionException e) {

                            assertTrue("EQ condition cannot have lower value than GTE value, conditionFirst SimpleCondition{type=EQ, field='int', value=50}, conditionSecond SimpleCondition{type=GTE, field='int', value=80}".equals(e.getMessage()) ||
                                    "GTE condition cannot have greater value than EQ value, conditionFirst SimpleCondition{type=GTE, field='int', value=80}, conditionSecond SimpleCondition{type=EQ, field='int', value=50}".equals(e.getMessage()));
                        }
                    }
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.EQ, "int", 80);
                        assertEquals(condition, conditionService.parse("AND(int GTE 80;int EQ 80)"));
                    }
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.EQ, "int", 90);
                        assertEquals(condition, conditionService.parse("AND(int GTE 80;int EQ 90)"));
                    }
                }
                // LIKE
                {
                    {
                        try {
                            conditionService.parse("AND(String LIKE xe;String EQ test)");
                            fail();
                        } catch (ConditionException e) {
                            assertTrue("LIKE condition cannot have other value than EQ value, conditionFirst SimpleCondition{type=LIKE, field='String', value=xe}, conditionSecond SimpleCondition{type=EQ, field='String', value=test}".equals(e.getMessage()) ||
                                    "EQ condition cannot have other value than LIKE value, conditionFirst SimpleCondition{type=EQ, field='String', value=test}, conditionSecond SimpleCondition{type=LIKE, field='String', value=xe}".equals(e.getMessage()));
                        }
                    }
                    {
                        try {
                            conditionService.parse("AND(String LIKE etest;String EQ test)");
                            fail();
                        } catch (ConditionException e) {
                            assertTrue("LIKE condition cannot have other value than EQ value, conditionFirst SimpleCondition{type=LIKE, field='String', value=etest}, conditionSecond SimpleCondition{type=EQ, field='String', value=test}".equals(e.getMessage()) ||
                                    "EQ condition cannot have other value than LIKE value, conditionFirst SimpleCondition{type=EQ, field='String', value=test}, conditionSecond SimpleCondition{type=LIKE, field='String', value=etest}".equals(e.getMessage()));
                        }
                    }
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.EQ, "String", "test");
                        assertEquals(condition, conditionService.parse("AND(String LIKE est;String EQ test)"));
                    }
                }
                // NOT
                {
                    {
                        try {
                            conditionService.parse("AND(String EQ test;String NOT test)");
                            fail();
                        } catch (ConditionException e) {
                            assertTrue("EQ condition cannot have same value as NOT value, conditionFirst SimpleCondition{type=EQ, field='String', value=test}, conditionSecond SimpleCondition{type=NOT, field='String', value=test}".equals(e.getMessage()) ||
                                    "NOT condition cannot have same value as EQ value, conditionFirst SimpleCondition{type=NOT, field='String', value=test}, conditionSecond SimpleCondition{type=EQ, field='String', value=test}".equals(e.getMessage()));
                        }
                    }
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.EQ, "String", "test");
                        assertEquals(condition, conditionService.parse("AND(String EQ test;String NOT testx)"));
                    }
                }
            }
            // LT
            {
                //EQ
                {
                    {
                        try {
                            conditionService.parse("AND(int EQ 50;int LT 20)");
                            fail();
                        } catch (ConditionException e) {
                            assertTrue("EQ condition cannot have greater or equal value than LT value, conditionFirst SimpleCondition{type=EQ, field='int', value=50}, conditionSecond SimpleCondition{type=LT, field='int', value=20}".equals(e.getMessage()) ||
                                    "LT condition cannot have lower or equal value than EQ value, conditionFirst SimpleCondition{type=LT, field='int', value=20}, conditionSecond SimpleCondition{type=EQ, field='int', value=50}".equals(e.getMessage()));
                        }
                    }
                    {
                        try {
                            conditionService.parse("AND(int EQ 20;int LT 20)");
                            fail();
                        } catch (ConditionException e) {
                            assertTrue("EQ condition cannot have greater or equal value than LT value, conditionFirst SimpleCondition{type=EQ, field='int', value=20}, conditionSecond SimpleCondition{type=LT, field='int', value=20}".equals(e.getMessage()) ||
                                    "LT condition cannot have lower or equal value than EQ value, conditionFirst SimpleCondition{type=LT, field='int', value=20}, conditionSecond SimpleCondition{type=EQ, field='int', value=20}".equals(e.getMessage()));
                        }
                    }
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.EQ, "int", 10);
                        assertEquals(condition, conditionService.parse("AND(int EQ 10;int LT 20)"));
                    }
                }
                // LT
                {
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LT, "int", 10);
                        assertEquals(condition, conditionService.parse("AND(int LT 10;int LT 10)"));
                    }
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LT, "int", 10);
                        assertEquals(condition, conditionService.parse("AND(int LT 20;int LT 10)"));
                    }
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LT, "int", 20);
                        assertEquals(condition, conditionService.parse("AND(int LT 20;int LT 30)"));
                    }
                }
                // LTE
                {
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LT, "int", 20);
                        assertEquals(condition, conditionService.parse("AND(int LTE 30;int LT 20)"));
                    }
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LT, "int", 20);
                        assertEquals(condition, conditionService.parse("AND(int LTE 20;int LT 20)"));
                    }
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LTE, "int", 10);
                        assertEquals(condition, conditionService.parse("AND(int LTE 10;int LT 20)"));
                    }
                }
                // GT
                {
                    {
                        try {
                            conditionService.parse("AND(int GT 50;int LT 10)");
                            fail();
                        } catch (ConditionException e) {
                            assertTrue("LT condition cannot have lower or equal value than GT value, conditionFirst SimpleCondition{type=LT, field='int', value=10}, conditionSecond SimpleCondition{type=GT, field='int', value=50}".equals(e.getMessage()) ||
                                    "GT condition cannot have greater or equal value than LT value, conditionSecond SimpleCondition{type=GT, field='int', value=50}, conditionSecond SimpleCondition{type=LT, field='int', value=10}".equals(e.getMessage()));
                        }
                    }
                    {
                        try {
                            conditionService.parse("AND(int GT 50;int LT 50)");
                            fail();
                        } catch (ConditionException e) {
                            assertTrue("LT condition cannot have lower or equal value than GT value, conditionFirst SimpleCondition{type=LT, field='int', value=50}, conditionSecond SimpleCondition{type=GT, field='int', value=50}".equals(e.getMessage()) ||
                                    "GT condition cannot have greater or equal value than LT value, conditionFirst SimpleCondition{type=GT, field='int', value=50}, conditionSecond SimpleCondition{type=LT, field='int', value=50}".equals(e.getMessage()));
                        }
                    }
                    {
                        final ICondition condition = FieldComplexCondition.make(ICondition.ComplexType.AND,
                                SimpleCondition.make(ICondition.SimpleType.GT, "int", 10),
                                SimpleCondition.make(ICondition.SimpleType.LT, "int", 50));
                        assertEquals(condition, conditionService.parse("AND(int GT 10;int LT 50)"));
                    }
                }
                // GTE
                {
                    {
                        try {
                            conditionService.parse("AND(int GTE 50;int LT 10)");
                            fail();
                        } catch (ConditionException e) {
                            assertTrue("LT condition cannot have lower or equal value than GTE value, conditionFirst SimpleCondition{type=LT, field='int', value=10}, conditionSecond SimpleCondition{type=GTE, field='int', value=50}".equals(e.getMessage()) ||
                                    "GTE condition cannot have greater or equal value than LT value, conditionSecond SimpleCondition{type=GTE, field='int', value=50}, conditionSecond SimpleCondition{type=LT, field='int', value=10}".equals(e.getMessage()));
                        }
                    }
                    {
                        try {
                            conditionService.parse("AND(int GTE 50;int LT 50)");
                            fail();
                        } catch (ConditionException e) {
                            assertTrue("LT condition cannot have lower or equal value than GTE value, conditionFirst SimpleCondition{type=LT, field='int', value=50}, conditionSecond SimpleCondition{type=GTE, field='int', value=50}".equals(e.getMessage()) ||
                                    "GTE condition cannot have greater or equal value than LT value, conditionFirst SimpleCondition{type=GTE, field='int', value=50}, conditionSecond SimpleCondition{type=LT, field='int', value=50}".equals(e.getMessage()));
                        }
                    }
                    {
                        final ICondition condition = FieldComplexCondition.make(ICondition.ComplexType.AND,
                                SimpleCondition.make(ICondition.SimpleType.GTE, "int", 10),
                                SimpleCondition.make(ICondition.SimpleType.LT, "int", 50));
                        assertEquals(condition, conditionService.parse("AND(int GTE 10;int LT 50)"));
                    }
                }
                // LIKE - doesn't affect
                {
                    {
                        final ICondition condition = FieldComplexCondition.make(ICondition.ComplexType.AND,
                                SimpleCondition.make(ICondition.SimpleType.LIKE, "String", "test"),
                                SimpleCondition.make(ICondition.SimpleType.LT, "String", "test"));
                        assertEquals(condition, conditionService.parse("AND(String LIKE test;String LT test)"));
                    }
                    {
                        final ICondition condition = FieldComplexCondition.make(ICondition.ComplexType.AND,
                                SimpleCondition.make(ICondition.SimpleType.LIKE, "String", "test1"),
                                SimpleCondition.make(ICondition.SimpleType.LT, "String", "test"));
                        assertEquals(condition, conditionService.parse("AND(String LIKE test1;String LT test)"));
                    }
                    {
                        final ICondition condition = FieldComplexCondition.make(ICondition.ComplexType.AND,
                                SimpleCondition.make(ICondition.SimpleType.LIKE, "String", "test"),
                                SimpleCondition.make(ICondition.SimpleType.LT, "String", "test1"));
                        assertEquals(condition, conditionService.parse("AND(String LIKE test;String LT test1)"));
                    }
                }
                // NOT
                {
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LT, "int", 10);
                        assertEquals(condition, conditionService.parse("AND(int LT 10;int NOT 50)"));
                    }
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LT, "int", 10);
                        assertEquals(condition, conditionService.parse("AND(int LT 10;int NOT 10)"));
                    }
                    {
                        final ICondition condition = FieldComplexCondition.make(ICondition.ComplexType.AND,
                                SimpleCondition.make(ICondition.SimpleType.LT, "int", 50),
                                SimpleCondition.make(ICondition.SimpleType.NOT, "int", 10));
                        assertEquals(condition, conditionService.parse("AND(int LT 50;int NOT 10)"));
                    }
                }
            }
            // LTE
            {
                // EQ
                {
                    {
                        try {
                            conditionService.parse("AND(int EQ 50;int LTE 20)");
                            fail();
                        } catch (ConditionException e) {
                            assertTrue("EQ condition cannot have greater value than LTE value, conditionFirst SimpleCondition{type=EQ, field='int', value=50}, conditionSecond SimpleCondition{type=LTE, field='int', value=20}".equals(e.getMessage()) ||
                                    "LTE condition cannot have lower value than EQ value, conditionFirst SimpleCondition{type=LTE, field='int', value=20}, conditionSecond SimpleCondition{type=EQ, field='int', value=50}".equals(e.getMessage()));
                        }
                    }
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.EQ, "int", 20);
                        assertEquals(condition, conditionService.parse("AND(int EQ 20;int LTE 20)"));
                    }
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.EQ, "int", 10);
                        assertEquals(condition, conditionService.parse("AND(int EQ 10;int LTE 20;)"));
                    }
                }
                // LT
                {
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LT, "int", 20);
                        assertEquals(condition, conditionService.parse("AND(int LT 20;int LTE 30)"));
                    }
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LT, "int", 20);
                        assertEquals(condition, conditionService.parse("AND(int LT 20;int LTE 20)"));
                    }
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LTE, "int", 10);
                        assertEquals(condition, conditionService.parse("AND(int LT 20;int LTE 10;)"));
                    }
                }
                // LTE
                {
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LTE, "int", 10);
                        assertEquals(condition, conditionService.parse("AND(int LTE 10;int LTE 10)"));
                    }
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LTE, "int", 10);
                        assertEquals(condition, conditionService.parse("AND(int LTE 20;int LTE 10)"));
                    }
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LTE, "int", 20);
                        assertEquals(condition, conditionService.parse("AND(int LTE 20;int LTE 30)"));
                    }
                }
                // GT
                {
                    {
                        try {
                            conditionService.parse("AND(int GT 50;int LTE 10)");
                            fail();
                        } catch (ConditionException e) {
                            assertTrue("LTE condition cannot have lower or equal value than GT value, conditionFirst SimpleCondition{type=LTE, field='int', value=10}, conditionSecond SimpleCondition{type=GT, field='int', value=50}".equals(e.getMessage()) ||
                                    "GT condition cannot have greater or equal value than LTE value, conditionFirst SimpleCondition{type=GT, field='int', value=50}, conditionSecond SimpleCondition{type=LTE, field='int', value=10}".equals(e.getMessage()));
                        }
                    }
                    {
                        try {
                            conditionService.parse("AND(int GT 50;int LTE 50)");
                            fail();
                        } catch (ConditionException e) {
                            assertTrue("LTE condition cannot have lower or equal value than GT value, conditionFirst SimpleCondition{type=LTE, field='int', value=50}, conditionSecond SimpleCondition{type=GT, field='int', value=50}".equals(e.getMessage()) ||
                                    "GT condition cannot have greater or equal value than LTE value, conditionFirst SimpleCondition{type=GT, field='int', value=50}, conditionSecond SimpleCondition{type=LTE, field='int', value=50}".equals(e.getMessage()));
                        }
                    }
                    {
                        final ICondition condition = FieldComplexCondition.make(ICondition.ComplexType.AND,
                                SimpleCondition.make(ICondition.SimpleType.GT, "int", 10),
                                SimpleCondition.make(ICondition.SimpleType.LTE, "int", 50));
                        assertEquals(condition, conditionService.parse("AND(int GT 10;int LTE 50)"));
                    }
                }
                // GTE
                {
                    {
                        try {
                            conditionService.parse("AND(int GTE 50;int LTE 10)");
                            fail();
                        } catch (ConditionException e) {
                            assertTrue("LTE condition cannot have lower value than GTE value, conditionFirst SimpleCondition{type=LTE, field='int', value=10}, conditionSecond SimpleCondition{type=GTE, field='int', value=50}".equals(e.getMessage()) ||
                                    "GTE condition cannot have greater value than LTE value, conditionFirst SimpleCondition{type=GTE, field='int', value=50}, conditionSecond SimpleCondition{type=LTE, field='int', value=10}".equals(e.getMessage()));
                        }
                    }
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.EQ, "int", 10);
                        assertEquals(condition, conditionService.parse("AND(int GTE 10;int LTE 10)"));
                    }
                    {
                        final ICondition condition = FieldComplexCondition.make(ICondition.ComplexType.AND,
                                SimpleCondition.make(ICondition.SimpleType.GTE, "int", 10),
                                SimpleCondition.make(ICondition.SimpleType.LTE, "int", 50));
                        assertEquals(condition, conditionService.parse("AND(int GTE 10;int LTE 50)"));
                    }
                }
                // LIKE - doesn't affect
                {
                    {
                        final ICondition condition = FieldComplexCondition.make(ICondition.ComplexType.AND,
                                SimpleCondition.make(ICondition.SimpleType.LIKE, "String", "test"),
                                SimpleCondition.make(ICondition.SimpleType.LTE, "String", "test"));
                        assertEquals(condition, conditionService.parse("AND(String LIKE test;String LTE test)"));
                    }
                    {
                        final ICondition condition = FieldComplexCondition.make(ICondition.ComplexType.AND,
                                SimpleCondition.make(ICondition.SimpleType.LIKE, "String", "test1"),
                                SimpleCondition.make(ICondition.SimpleType.LTE, "String", "test"));
                        assertEquals(condition, conditionService.parse("AND(String LIKE test1;String LTE test)"));
                    }
                    {
                        final ICondition condition = FieldComplexCondition.make(ICondition.ComplexType.AND,
                                SimpleCondition.make(ICondition.SimpleType.LIKE, "String", "test"),
                                SimpleCondition.make(ICondition.SimpleType.LTE, "String", "test1"));
                        assertEquals(condition, conditionService.parse("AND(String LIKE test;String LTE test1)"));
                    }
                }
                // NOT
                {
                    {
                        final ICondition condition = SimpleCondition.make(ICondition.SimpleType.LTE, "int", 10);
                        assertEquals(condition, conditionService.parse("AND(int LTE 10;int NOT 50)"));
                    }
                    {
                        final ICondition condition = FieldComplexCondition.make(ICondition.ComplexType.AND,
                                SimpleCondition.make(ICondition.SimpleType.LTE, "int", 10),
                                SimpleCondition.make(ICondition.SimpleType.NOT, "int", 10));
                        assertEquals(condition, conditionService.parse("AND(int LTE 10;int NOT 10)"));
                    }
                    {
                        final ICondition condition = FieldComplexCondition.make(ICondition.ComplexType.AND,
                                SimpleCondition.make(ICondition.SimpleType.LTE, "int", 50),
                                SimpleCondition.make(ICondition.SimpleType.NOT, "int", 10));
                        assertEquals(condition, conditionService.parse("AND(int LTE 50;int NOT 10)"));
                    }
                }
            }
        }
        {
            final ICondition condition = SimpleCondition.make(ICondition.SimpleType.EQ, "int", 50);
            assertEquals(condition, conditionService.parse("AND(int GT 30;int EQ 50)"));
        }
        {
            final ICondition condition = FieldComplexCondition.make(ICondition.ComplexType.AND,
                    SimpleCondition.make(ICondition.SimpleType.GT, "int", 50),
                    SimpleCondition.make(ICondition.SimpleType.LT, "int", 80));
            assertEquals(condition, conditionService.parse("AND(int GT 30;int GT 40;int GT 50;int LT 100;int LT 90;int LT 80)"));
        }
    }
}