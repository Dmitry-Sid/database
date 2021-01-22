import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sample.model.ApplicationConverter;
import sample.model.ApplicationConverterImpl;
import sample.model.Repository;
import sample.model.RepositoryImpl;
import sample.model.pojo.Application;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class RepositoryTest {
    private final static String fileName = "diff";
    private final ApplicationConverter applicationConverter = new ApplicationConverterImpl();
    private Repository repository;
    private static final byte[] markBytes = "mark".getBytes();
    private final static int size = 5;

    @Before
    public void before() {
        prepareFile(size);
        repository = new RepositoryImpl(fileName, applicationConverter);
    }

    @After
    public void after() {
        new File(fileName).delete();
    }

    @Test
    public void getTest() {
        prepareFile(size);
        final List<Application> applications = getAppList(size);
        for (int i = 0; i < size; i++) {
            assertEquals(applications.get(i), repository.get("TEST" + i));
        }
        assertNull(repository.get("TEST" + size + 1));
    }

    @Test
    public void addTest() {
        {
            final Application application = BaseApplicationTest.generateApplication("TEST" + 6, 6);
            repository.add(application);
            assertEquals(application, repository.get("TEST6"));
            assertEquals(BaseApplicationTest.generateApplication("TEST1", 1), repository.get("TEST1"));
        }
        {
            final Application application = BaseApplicationTest.generateApplication("TEST" + 7, 7);
            repository.add(application);
            assertEquals(application, repository.get("TEST7"));
            assertEquals(BaseApplicationTest.generateApplication("TEST6", 6), repository.get("TEST6"));
        }
        {
            final Application application = repository.get("TEST1");
            application.getPersonInfo().setFirstName("firstNameTest");
            repository.add(application);
            assertEquals(application, repository.get("TEST1"));
            assertEquals(BaseApplicationTest.generateApplication("TEST3", 3), repository.get("TEST3"));
            assertEquals(BaseApplicationTest.generateApplication("TEST6", 6), repository.get("TEST6"));
        }
        {
            final Application application = repository.get("TEST4");
            application.getPersonInfo().setFirstName("firstNameTest4");
            repository.add(application);
            assertEquals(application, repository.get("TEST4"));
            assertNotEquals(BaseApplicationTest.generateApplication("TEST1", 1), repository.get("TEST1"));
            assertEquals(BaseApplicationTest.generateApplication("TEST6", 6), repository.get("TEST6"));
            assertEquals(BaseApplicationTest.generateApplication("TEST7", 7), repository.get("TEST7"));
        }
        {
            final Repository testRepository = new RepositoryImpl(fileName, applicationConverter);
            assertEquals(BaseApplicationTest.generateApplication("TEST6", 6), testRepository.get("TEST6"));
            assertEquals(BaseApplicationTest.generateApplication("TEST7", 7), testRepository.get("TEST7"));
        }
    }

    @Test
    public void deleteTest() {
        final List<Application> applications = getAppList(size);
        assertEquals(applications.get(1), repository.get("TEST1"));
        repository.delete("TEST1");
        assertEquals(applications.get(0), repository.get("TEST0"));
        assertNull(repository.get("TEST1"));
        for (int i = 2; i < size - 2; i++) {
            assertEquals(applications.get(i), repository.get("TEST" + i));
        }
        repository.delete("TEST4");
        repository.delete("TEST0");
        assertNull(repository.get("TEST4"));
        assertNull(repository.get("TEST0"));
        assertEquals(applications.get(2), repository.get("TEST" + 2));
        assertEquals(applications.get(3), repository.get("TEST" + 3));
        final Repository testRepository = new RepositoryImpl(fileName, applicationConverter);
        assertEquals(applications.get(2), testRepository.get("TEST" + 2));
        assertEquals(applications.get(3), testRepository.get("TEST" + 3));
    }

    @Test
    public void timeDeleteTest() {
        final List<Long> calculationList = new ArrayList<>();
        final int size = 10;
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < size; j++) {
                prepareFile(i + 2);
                final Repository testRepository = new RepositoryImpl(fileName, applicationConverter);
                assertEquals(BaseApplicationTest.generateApplication("TEST" + 1, 1), testRepository.get("TEST1"));
                long begin = System.currentTimeMillis();
                testRepository.delete("TEST1");
                long end = System.currentTimeMillis();
                if (calculationList.size() < i + 1) {
                    calculationList.add((end - begin) / size);
                } else {
                    calculationList.set(i, calculationList.get(i) + (end - begin) / size);
                }
                assertNull(testRepository.get("TEST1"));
            }
        }
        for (Long time : calculationList) {
            System.out.println(time);
        }
    }

    private void prepareFile(int size) {
        try (FileOutputStream fous = new FileOutputStream(fileName)) {
            for (Application application : getAppList(size)) {
                fous.write(markBytes);
                fous.write(applicationConverter.toBytes(application));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<Application> getAppList(int size) {
        final List<Application> applicationList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            applicationList.add(BaseApplicationTest.generateApplication("TEST" + i, i));
        }
        return applicationList;
    }


}
