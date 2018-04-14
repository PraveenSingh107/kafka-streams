package com.praveen.kafka.frauddetection;

import java.sql.Timestamp;

public class Transaction {

    private int posID;
    private String transactionId;
    private String cardNumber;
    private Timestamp transactionTime;
    private Float transactionAmount;
    private String merchantCategoryCode;
    private String locationZone;

    public int getPosID() {
        return posID;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public String getCardNumber() {
        return cardNumber;
    }

    public Timestamp getTransactionTime() {
        return transactionTime;
    }

    public Float getTransactionAmount() {
        return transactionAmount;
    }

    public String getMerchantCategoryCode() {
        return merchantCategoryCode;
    }

    public String getLocationZone() {
        return locationZone;
    }

    public Transaction() {}

    @Override
    public String toString() {
        return "Transaction{" +
                "posID=" + posID +
                ", transactionId='" + transactionId + '\'' +
                ", cardNumber='" + cardNumber + '\'' +
                ", transactionTime=" + transactionTime +
                ", transactionAmount=" + transactionAmount +
                ", merchantCategoryCode='" + merchantCategoryCode + '\'' +
                ", locationZone='" + locationZone + '\'' +
                '}';
    }

    public Transaction(int posID, String transactionId, String cardNumber,
                       Timestamp transactionTime, float transactionAmount,
                       String merchantCategoryCode,
                       String locationZone) {
        this.posID = posID;
        this.transactionId = transactionId;
        this.cardNumber = cardNumber;
        this.transactionTime = transactionTime;
        this.transactionAmount = transactionAmount;
        this.merchantCategoryCode = merchantCategoryCode;
        this.locationZone = locationZone;
    }
}
