
import sample.model.pojo.Row;

import java.util.HashMap;

public class TestUtils {
    public static Row generateRow(int id, int intValue) {
        return new Row(id, new HashMap<String, Object>(){{put("field" + intValue, intValue);}});
    }

}
