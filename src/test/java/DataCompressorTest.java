import org.junit.Test;
import server.model.DataCompressor;
import server.model.impl.DataCompressorImpl;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertTrue;

public class DataCompressorTest {

    @Test
    public void fullTest() {
        final DataCompressor dataCompressor = new DataCompressorImpl();
        final byte[] input = "test text for compressing test text for compressing test text for compressing test text for compressing test text for compressing".getBytes(StandardCharsets.UTF_8);
        final byte[] output = dataCompressor.compress(input);
        assertTrue(input.length > output.length);
        TestUtils.assertBytes(input, dataCompressor.decompress(output));
    }

}
