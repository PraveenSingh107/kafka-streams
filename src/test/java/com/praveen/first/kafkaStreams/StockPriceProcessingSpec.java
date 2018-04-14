package com.praveen.first.kafkaStreams;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

import static org.hamcrest.CoreMatchers.is;

public class StockPriceProcessingSpec {

    @Test
    public void shouldFilterStockQuoteFeed() throws Exception {
        StockPriceProducer.produce(100, 0);
        KafkaStreams stream = StockPriceProcessing.createStream();
        stream.start();

        ArrayList<ConsumerRecord<String, StockQuote>> processedStockFeeds =
                StockPriceConsumer.consume(10);
        Assert.assertThat(processedStockFeeds.size(), is(100));
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
    }

    @Test
    public void shouldReturnStockLiquidityData() throws Exception {
        StockPriceProducer.produce(100, 0);
        KafkaStreams stream = StockPriceProcessing.createStream();
        stream.start();

        ArrayList<ConsumerRecord<String, Long>> processedStockFeeds =
                StockPriceConsumer.getStockLiquidityData(10);
        Assert.assertTrue(processedStockFeeds.size() <= 6);
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
    }


}
