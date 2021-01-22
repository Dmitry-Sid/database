package sample.model.pojo;

import java.io.Serializable;
import java.util.Objects;

public class Application implements Serializable {
    private static final long serialVersionUID = -6678049336684033088L;
    private String id;
    private PersonInfo personInfo;

    public Application() {

    }

    public Application(String id, PersonInfo personInfo) {
        this.id = id;
        this.personInfo = personInfo;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public PersonInfo getPersonInfo() {
        return personInfo;
    }

    public void setPersonInfo(PersonInfo personInfo) {
        this.personInfo = personInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Application)) {
            return false;
        }
        final Application that = (Application) o;
        return Objects.equals(getId(), that.getId()) &&
                Objects.equals(getPersonInfo(), that.getPersonInfo());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getPersonInfo());
    }
}
