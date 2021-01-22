import sample.model.pojo.Address;
import sample.model.pojo.Application;
import sample.model.pojo.PersonInfo;

public class BaseApplicationTest {
    public static Application generateApplication(String id, int intValue) {
        return new Application(id, new PersonInfo("firstName" + intValue,
                "secondName" + intValue, "lastName" + intValue, new Address("city" + intValue, "street" + intValue, "building" + intValue, intValue), "8910409426" + intValue));
    }

}
