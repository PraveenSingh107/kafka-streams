package com.praveen.kafka.frauddetection;

import org.apache.kafka.common.serialization.Serde;

import java.util.Map;


public class POSTransactionStatSerdes implements Serde<Aggregated> {

    private TrnxStatsSerializer serializer;
    private TrnxStatsDeserializer deserializer;

    public POSTransactionStatSerdes() {
        this.serializer = new TrnxStatsSerializer();
        this.deserializer = new TrnxStatsDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public TrnxStatsSerializer serializer() {
        return serializer;
    }

    @Override
    public TrnxStatsDeserializer deserializer() {
        return deserializer;
    }
}