package com.praveen.kafka.frauddetection;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.tools.internal.xjc.reader.xmlschema.bindinfo.BIConversion;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class UserRecordDeserializer implements Deserializer<UserRecord> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public UserRecord deserialize(String topic, byte[] data) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true);
        UserRecord userRecord = null;
        try {
            if (data == null || data.length == 0)
                return null;
            userRecord = objectMapper.readValue(data, UserRecord.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return userRecord;
    }

    @Override
    public void close() {

    }
}
