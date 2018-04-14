package com.praveen.kafka.frauddetection;

public class UserRecord {

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getEmail() {
        return email;
    }

    public String getCountryOfResidence() {
        return countryOfResidence;
    }

    public String getCardNumber() {
        return cardNumber;
    }

    public int getUserId() {
        return userId;
    }

    public UserRecord(int userId, String firstName, String lastName, String email, String countryOfResidence, String cardNumber) {
        this.userId = userId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.countryOfResidence = countryOfResidence;
        this.cardNumber = cardNumber;
    }

    public UserRecord() {
    }

    private int userId;
    private String firstName;
    private String lastName;
    private String email;
    private String countryOfResidence;

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public void setCountryOfResidence(String countryOfResidence) {
        this.countryOfResidence = countryOfResidence;
    }

    public void setCardNumber(String cardNumber) {
        this.cardNumber = cardNumber;
    }

    private String cardNumber;

}
