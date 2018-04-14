package com.praveen.first.kafkaStreams;

import com.first_stream_app.ConsumerClient;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class StockPriceConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerClient.class);

    public static ArrayList<ConsumerRecord<String, StockQuote>> consume(int timeoutInSec) {
        Consumer<String, StockQuote> consumer = getConsumer();
        long startTime = System.currentTimeMillis();

        ArrayList<ConsumerRecord<String, StockQuote>> consumedRecords = new ArrayList<>();

        while ((System.currentTimeMillis() - startTime) < (timeoutInSec * 1000)) {
            final ConsumerRecords<String, StockQuote> records = consumer.poll(100);

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

    public static ArrayList<ConsumerRecord<String, Long>> getStockLiquidityData(int timeoutInSec) {
        Consumer<String, Long> consumer = getLiquidityConsumer();
        long startTime = System.currentTimeMillis();

        ArrayList<ConsumerRecord<String, Long>> consumedRecords = new ArrayList<>();

        while ((System.currentTimeMillis() - startTime) < (timeoutInSec * 1000)) {
//            logger.info("before polling");
            final ConsumerRecords<String, Long> records = consumer.poll(100);
//            logger.info("polling results");
            records.forEach(
                    record -> {
                        logger.info("Message received: topic={}, key={}, value={}, offset={}, partition={}",
                                record.topic(), record.key(), record.value(), record.offset());
                        consumedRecords.add(record);
                    });
            consumer.commitSync();
//            logger.info("commit sync done.");
        }

        consumer.close();
        return consumedRecords;
    }

    private static Consumer<String, StockQuote> getConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, StockPriceConstants.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "stock-price-feeds-processing-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StockQuoteDeserializer.class.getName());

        KafkaConsumer<String, StockQuote> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(StockPriceConstants.STOCK_PRICE_FEED_PROCESSING_TOPIC));
        return consumer;
    }

    private static Consumer<String, Long> getLiquidityConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, StockPriceConstants.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "stock-price-feeds-liquidity-processing-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

        KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(StockPriceConstants.STOCK_LIQUIDITY_TOPIC));
        return consumer;
    }
}
