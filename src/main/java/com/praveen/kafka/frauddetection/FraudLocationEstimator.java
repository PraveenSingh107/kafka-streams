package com.praveen.kafka.frauddetection;

import java.util.HashMap;
import java.util.List;

public class FraudLocationEstimator {

    private static HashMap<String, Float> locationIndex = new HashMap<>();

    static {
        locationIndex.put("1234512345", 9.0F);
        locationIndex.put("5432154321", 4.0F);
        locationIndex.put("9876598765", 1.0F);
    }

    public static float getFraudLocationIndex(List<EnrichedTransaction> transactionList) {
        return locationIndex.get(transactionList.get(0).getTransaction().getCardNumber());
    }
}
