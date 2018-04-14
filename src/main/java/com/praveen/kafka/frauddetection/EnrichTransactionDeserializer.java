package com.praveen.kafka.frauddetection;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class EnrichTransactionDeserializer implements Deserializer<EnrichedTransaction> {
    public EnrichTransactionDeserializer() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public EnrichedTransaction deserialize(String topic, byte[] data) {
        ObjectMapper objectMapper = new ObjectMapper();
        EnrichedTransaction enrichedTransaction = null;
        try {
            enrichedTransaction = objectMapper.readValue(data, EnrichedTransaction.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return enrichedTransaction;
    }

    @Override
    public void close() {

    }
}
