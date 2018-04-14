package com.praveen.kafka.frauddetection;

public class EnrichedTransaction {
    private UserRecord userRecord;
    private Transaction transaction;

    public UserRecord getUserRecord() {
        return userRecord;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public EnrichedTransaction(UserRecord userRecord, Transaction transaction) {
        this.userRecord = userRecord;
        this.transaction = transaction;
    }

    public EnrichedTransaction() {
    }

    @Override
    public String toString() {
        return "EnrichedTransaction{" +
                "userRecord=" + userRecord.toString() +
                ", transaction=" + transaction.toString() +
                '}';
    }
}

