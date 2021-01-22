import org.junit.Test;
import sample.model.ApplicationConverter;
import sample.model.ApplicationConverterImpl;
import sample.model.pojo.Application;

import static org.junit.Assert.assertEquals;

public class ApplicationConverterTest extends BaseApplicationTest {
    private final ApplicationConverter applicationConverter = new ApplicationConverterImpl();

    @Test
    public void fullTest() {
        final Application application1 = BaseApplicationTest.generateApplication("1", 1);
        assertEquals(application1, applicationConverter.fromBytes(applicationConverter.toBytes(application1)));
    }


}
