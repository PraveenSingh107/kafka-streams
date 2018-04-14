package com.praveen.first.kafkaStreams;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class StockQuoteDeserializer implements Deserializer<StockQuote> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public StockQuote deserialize(String topic, byte[] data) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true);
        StockQuote stockQuote = null;
        try {
            stockQuote = objectMapper.readValue(data, StockQuote.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return stockQuote;
    }

    @Override
    public void close() {

    }
}
