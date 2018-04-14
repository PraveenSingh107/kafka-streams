package com.praveen.kafka.frauddetection;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class POSTransactionConsumer {

    private static final Logger logger = LoggerFactory.getLogger(POSTransactionConsumer.class);

    public static ArrayList<ConsumerRecord<String, Transaction>> consume(int timeInSecs) {
        KafkaConsumer<String, Transaction> consumer = createConsumer();
        long startTime = System.currentTimeMillis();
        ArrayList<ConsumerRecord<String, Transaction>> consumedRecords = new ArrayList<>();
        while ((System.currentTimeMillis() - startTime) < (timeInSecs * 1000)) {
            final ConsumerRecords<String, Transaction> records = consumer.poll(100);
            records.forEach(
                    record -> {
                        logger.info("Message received: topic={}, key={}, value={}, offset={}, partition={}",
                                record.topic(), record.key(), record.value(), record.offset());
                        consumedRecords.add(record);
                    });
            consumer.commitSync();
        }
        consumer.close();
        return consumedRecords;
    }

    public static KafkaConsumer<String, Transaction> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.FRAUD_DETECTION_BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TransactionDeserializer.class.getName());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, Constants.FRAUD_DETECTION_CLIENT_ID_CONFIG);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "stock-price-feeds-processing-group");
        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(Constants.FRAUD_TRANSACTIONS_TOPIC));
        return consumer;
    }
}
