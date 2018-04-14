package com.praveen.kafka.frauddetection;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class TrnxStatsDeserializer implements Deserializer<Aggregated> {
    public TrnxStatsDeserializer() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Aggregated deserialize(String topic, byte[] data) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true);
        Aggregated aggregated = null;
        try {
            aggregated = objectMapper.readValue(data, Aggregated.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return aggregated;
    }

    @Override
    public void close() {

    }
}
