package com.praveen.kafka.frauddetection;

public class UserHistory {
    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getCardNumber() {
        return cardNumber;
    }

    public void setCardNumber(String cardNumber) {
        this.cardNumber = cardNumber;
    }

    public float getAvgTrnxAmount() {
        return avgTransactionAmount;
    }

    public void setAvgTransactionAmount(float avgTransactionAmount) {
        this.avgTransactionAmount = avgTransactionAmount;
    }

    public float getMaxTransactionAmount() {
        return maxTransactionAmount;
    }

    public void setMaxTransactionAmount(float maxTransactionAmount) {
        this.maxTransactionAmount = maxTransactionAmount;
    }

    public float getAvgNoOfTransactions() {
        return avgNoOfTransactions;
    }

    public void setAvgNoOfTransactions(float avgNoOfTransactions) {
        this.avgNoOfTransactions = avgNoOfTransactions;
    }

    private int userId;
    private String cardNumber;
    private float avgTransactionAmount;
    private float maxTransactionAmount;
    private float avgNoOfTransactions;

    public UserHistory(int userId, String cardNumber, float avgTransactionAmount,
                       float maxTransactionAmount, float avgNoOfTransactions) {
        this.userId = userId;
        this.cardNumber = cardNumber;
        this.avgTransactionAmount = avgTransactionAmount;
        this.maxTransactionAmount = maxTransactionAmount;
        this.avgNoOfTransactions = avgNoOfTransactions;
    }

    public UserHistory() {
    }

    @Override
    public String toString() {
        return "UserHistory{" +
                "userId=" + userId +
                ", cardNumber='" + cardNumber + '\'' +
                ", avgTransactionAmount=" + avgTransactionAmount +
                ", maxTransactionAmount=" + maxTransactionAmount +
                ", avgNoOfTransactions=" + avgNoOfTransactions +
                '}';
    }
}
