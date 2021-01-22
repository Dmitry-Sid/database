package sample.model.pojo;

import java.io.Serializable;
import java.util.Objects;

public class PersonInfo implements Serializable {
    private static final long serialVersionUID = -3577518910784918187L;
    private String firstName;
    private String secondName;
    private String lastName;
    private Address address;
    private String phone;

    public PersonInfo(String firstName, String secondName, String lastName, Address address, String phone) {
        this.firstName = firstName;
        this.secondName = secondName;
        this.lastName = lastName;
        this.address = address;
        this.phone = phone;
    }

    public PersonInfo() {

    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getSecondName() {
        return secondName;
    }

    public void setSecondName(String secondName) {
        this.secondName = secondName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public Address getAddress() {
        return address;
    }

    public void setAddress(Address address) {
        this.address = address;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PersonInfo)) {
            return false;
        }
        final PersonInfo that = (PersonInfo) o;
        return Objects.equals(getFirstName(), that.getFirstName()) &&
                Objects.equals(getSecondName(), that.getSecondName()) &&
                Objects.equals(getLastName(), that.getLastName()) &&
                Objects.equals(getAddress(), that.getAddress()) &&
                Objects.equals(getPhone(), that.getPhone());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getFirstName(), getSecondName(), getLastName(), getAddress(), getPhone());
    }
}
