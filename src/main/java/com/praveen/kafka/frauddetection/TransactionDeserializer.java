package com.praveen.kafka.frauddetection;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class TransactionDeserializer implements Deserializer<Transaction> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Transaction deserialize(String topic, byte[] data) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true);
        Transaction transaction = null;
        try {
            transaction = objectMapper.readValue(data, Transaction.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return transaction;
    }

    @Override
    public void close() {

    }
}
