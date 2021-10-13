import org.junit.Test;
import server.model.ObjectConverter;
import server.model.impl.DataCompressorImpl;
import server.model.impl.ObjectConverterImpl;
import server.model.pojo.Row;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ObjectConverterTest {
    private final ObjectConverter objectConverter = new ObjectConverterImpl(new DataCompressorImpl());

    @Test
    public void bytesTest() {
        final Row row = TestUtils.generateRow(1, 1);
        assertEquals(row, objectConverter.fromBytes(Row.class, objectConverter.toBytes(row)));
    }

    @Test
    public void fileTest() {
        {
            final Row row = TestUtils.generateRow(1, 1);
            final String fileName = "row.temp";
            objectConverter.toFile(row, fileName);
            assertEquals(row, objectConverter.fromFile(Row.class, fileName));
            assertTrue(new File(fileName).delete());
        }
        {
            final Map<Integer, String> map = new HashMap<>();
            map.put(1, "temp1");
            map.put(2, "temp2");
            final String fileName = "map.temp";
            objectConverter.toFile((Serializable) map, fileName);
            assertEquals(map, objectConverter.fromFile(HashMap.class, fileName));
            assertTrue(new File(fileName).delete());
        }
    }

    @Test
    public void cloneTest() {
        final Row row = TestUtils.generateRow(1, 1);
        final Row clone = objectConverter.clone(row);
        assertNotSame(row, clone);
        assertEquals(row, clone);
    }

}
