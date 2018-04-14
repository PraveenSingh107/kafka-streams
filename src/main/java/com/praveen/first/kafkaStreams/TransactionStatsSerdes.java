package com.praveen.first.kafkaStreams;

import com.praveen.kafka.frauddetection.Aggregated;
import com.praveen.kafka.frauddetection.TrnxStatsDeserializer;
import com.praveen.kafka.frauddetection.TrnxStatsSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;


public class TransactionStatsSerdes implements Serde<Aggregated> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<Aggregated> serializer() {
        return new TrnxStatsSerializer();
    }

    @Override
    public Deserializer<Aggregated> deserializer() {
        return new TrnxStatsDeserializer();
    }
}
