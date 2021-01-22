package sample.model.pojo;

import java.io.Serializable;
import java.util.Objects;

public class Address implements Serializable {
    private static final long serialVersionUID = 4919674659317811932L;
    private String city;
    private String street;
    private String building;
    private int flat;

    public Address() {

    }

    public Address(String city, String street, String building, int flat) {
        this.city = city;
        this.street = street;
        this.building = building;
        this.flat = flat;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public String getBuilding() {
        return building;
    }

    public void setBuilding(String building) {
        this.building = building;
    }

    public int getFlat() {
        return flat;
    }

    public void setFlat(int flat) {
        this.flat = flat;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Address)) {
            return false;
        }
        final Address address = (Address) o;
        return getFlat() == address.getFlat() &&
                Objects.equals(getCity(), address.getCity()) &&
                Objects.equals(getStreet(), address.getStreet()) &&
                Objects.equals(getBuilding(), address.getBuilding());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getCity(), getStreet(), getBuilding(), getFlat());
    }
}
