import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import server.model.BaseFilePathHolder;
import server.model.TableServiceFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class TableServiceFactoryTest {

    @Test
    public void fullTest() {
        final ApplicationContext applicationContext = new ClassPathXmlApplicationContext("application-context.xml");
        {
            final TestClass1 testClass1 = applicationContext.getBean(TestClass1.class);
            final TestClass2 testClass2 = applicationContext.getBean(TestClass2.class);
            assertSame(testClass1, testClass2.testClass1);
            assertEquals("", testClass1.getFilePath());
            assertFalse(testClass1.init);
            assertEquals("", testClass2.getFilePath());
            assertFalse(testClass2.init);
        }
        final TableServiceFactory tableServiceFactory = (TableServiceFactory) applicationContext.getBean("tableServiceFactory");
        final AtomicReference<Map<Class<?>, Object>> mapAtomicReference1 = new AtomicReference<>();
        {
            tableServiceFactory.createServices("test", mapAtomicReference1::set, TestClass1.class, TestClass2.class);
            final TestClass1 testClass1 = (TestClass1) mapAtomicReference1.get().get(TestClass1.class);
            final TestClass2 testClass2 = (TestClass2) mapAtomicReference1.get().get(TestClass2.class);
            assertSame(testClass1, testClass2.testClass1);
            assertEquals("test/", testClass1.getFilePath());
            assertTrue(testClass1.init);
            assertEquals("test/", testClass2.getFilePath());
            assertTrue(testClass2.init);
        }
        final AtomicReference<Map<Class<?>, Object>> mapAtomicReference2 = new AtomicReference<>();
        {
            tableServiceFactory.createServices("test2", mapAtomicReference2::set, TestClass1.class, TestClass2.class);
            final TestClass1 testClass1 = (TestClass1) mapAtomicReference2.get().get(TestClass1.class);
            final TestClass2 testClass2 = (TestClass2) mapAtomicReference2.get().get(TestClass2.class);
            assertSame(testClass1, testClass2.testClass1);
            assertEquals("test2/", testClass1.getFilePath());
            assertTrue(testClass1.init);
            assertEquals("test2/", testClass2.getFilePath());
            assertTrue(testClass2.init);
            assertEquals("test/", ((TestClass1) mapAtomicReference1.get().get(TestClass1.class)).getFilePath());
            assertEquals("test/", ((TestClass2) mapAtomicReference1.get().get(TestClass2.class)).getFilePath());
            assertNotSame(testClass1, mapAtomicReference1.get().get(TestClass1.class));
            assertNotSame(testClass2, mapAtomicReference1.get().get(TestClass2.class));
        }
    }

    private abstract static class BaseClass extends BaseFilePathHolder {
        public final boolean init;

        private BaseClass(String filePath, boolean init) {
            super(filePath);
            this.init = init;
        }
    }

    private static class TestClass1 extends BaseClass {

        private TestClass1(String filePath, boolean init) {
            super(filePath, init);
        }
    }

    private static class TestClass2 extends BaseClass {
        private final TestClass1 testClass1;

        private TestClass2(String filePath, boolean init, TestClass1 testClass1) {
            super(filePath, init);
            this.testClass1 = testClass1;
        }
    }
}
