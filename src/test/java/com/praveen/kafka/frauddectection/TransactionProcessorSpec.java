package com.praveen.kafka.frauddectection;

import com.praveen.kafka.frauddetection.Transaction;
import com.praveen.kafka.frauddetection.POSTransactionConsumer;
import com.praveen.kafka.frauddetection.POSTransactionProcessor;
import com.praveen.kafka.frauddetection.POSTransactionProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class TransactionProcessorSpec {

    @Test
    public void shouldFilterStockQuoteFeed() throws Exception {
        POSTransactionProducer.produce();

        KafkaStreams stream = POSTransactionProcessor.createStream();
        stream.start();
        ArrayList<ConsumerRecord<String, Transaction>> fraudTrnasactions = POSTransactionConsumer.consume(10);
        Assert.assertTrue(fraudTrnasactions.size() == 15    );
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
    }

}
