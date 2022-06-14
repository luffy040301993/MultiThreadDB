package model;

import java.util.Date;

public class Book {

    private String id;
    private String LastName;
    private String FirstName;
    private String BirthDay;
    private String Classa;
    private String Type;
    private String Majors;
    public Book() {

    }

    public Book(String id,String LastName, String FirstName, String BirthDay, String Classa, String Type, String Majors ) {
        this.id = id;
        this.LastName = LastName;
        this.FirstName = FirstName;
        this.BirthDay = BirthDay;
        this.Classa = Classa;
        this.Type = Type;
        this.Majors = Majors;
    }
    public String getId() {
        return id;
    }

    public String getLastName() {
        return LastName;
    }

    public String getFirstName() {
        return FirstName;
    }

    public String getBirthDay() {
        return BirthDay;
    }

    public String getClassa() {
        return Classa;
    }

    public String getType() {
        return Type;
    }

    public String getMajors() {
        return Majors;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setLastName(String lastName) {
        LastName = lastName;
    }

    public void setFirstName(String firstName) {
        FirstName = firstName;
    }

    public void setBirthDay(String birthDay) {
        BirthDay = birthDay;
    }

    public void setClassa(String classa) {
        Classa = classa;
    }

    public void setType(String type) {
        Type = type;
    }

    public void setMajors(String majors) {
        Majors = majors;
    }
}