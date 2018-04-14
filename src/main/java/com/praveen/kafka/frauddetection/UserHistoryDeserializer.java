package com.praveen.kafka.frauddetection;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class UserHistoryDeserializer implements Deserializer<UserHistory> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public UserHistory deserialize(String topic, byte[] data) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true);
        UserHistory userHistory = null;
        try {
            if (data == null || data.length == 0)
                return  null;
            userHistory = objectMapper.readValue(data, UserHistory.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return userHistory;
    }

    @Override
    public void close() {

    }
}
