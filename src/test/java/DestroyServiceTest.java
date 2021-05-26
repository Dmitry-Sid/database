import org.junit.Test;
import server.model.DestroyService;
import server.model.Destroyable;
import server.model.impl.DestroyServiceImpl;

import static org.junit.Assert.assertEquals;

public class DestroyServiceTest {

    @Test
    public void fullTest() throws InterruptedException {
        final int[] values = {0, 0};
        final Destroyable destroyable1 = () -> values[0]++;
        final Destroyable destroyable2 = () -> values[1]++;
        assertEquals(0, values[0]);
        assertEquals(0, values[1]);
        final DestroyService destroyService = new DestroyServiceImpl(1000);
        destroyService.register(destroyable1);
        assertEquals(0, values[0]);
        assertEquals(0, values[1]);
        Thread.sleep(1200);
        assertEquals(1, values[0]);
        assertEquals(0, values[1]);
        destroyService.register(destroyable2);
        Thread.sleep(1200);
        assertEquals(2, values[0]);
        assertEquals(1, values[1]);
        destroyService.unregister(destroyable1);
        Thread.sleep(1200);
        assertEquals(2, values[0]);
        assertEquals(2, values[1]);
    }
}
