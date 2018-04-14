package com.praveen.kafka.frauddetection;

import java.util.Arrays;
import java.util.List;

public class TransactionFraudDetector {

    private static List<String> fradulentCreditCards = Arrays.asList("1234512345");

    public static Boolean isPotentialFraudulentTransaction(Transaction transaction) {
        return fradulentCreditCards.contains(transaction.getCardNumber());
    }

}

