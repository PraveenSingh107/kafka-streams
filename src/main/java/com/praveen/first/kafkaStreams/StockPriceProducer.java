package com.praveen.first.kafkaStreams;

import com.first_stream_app.ProducerClient;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.sql.Timestamp;

public class StockPriceProducer {

    private static final Logger logger = LoggerFactory.getLogger(ProducerClient.class);

    private static HashMap<Integer, String> symbols = new HashMap<>();
    private static HashMap<Integer, Float> priceLowRange = new HashMap<>();
    private static HashMap<Integer, Float> priceHighRange = new HashMap<>();
    private static HashMap<Integer, Float> lastVolume = new HashMap<>();
    private static HashMap<Integer, Float> priceHigh = new HashMap<>();
    private static HashMap<Integer, Float> priceLow = new HashMap<>();
    private static Random random = new Random();

    public static void produce(int tickers, long tickerIntervlInMS) {
        Producer<String, StockQuote> producer = createStream();
        while (tickers-- > 0) {
            try {
                int symbolIndex = random.nextInt(6);
                ProducerRecord<String, StockQuote> record =
                        new ProducerRecord<>(
                                StockPriceConstants.STOCK_PRICE_FEED_TOPIC,
                                symbols.get(symbolIndex).toString(),
                                getTickerFeed(symbolIndex)
                                );
                RecordMetadata recordMetadata = producer.send(record).get();
                logger.info("Ticker sent: key={}, value={}, offset={}, partition={}", record.key(),
                        record.value(), recordMetadata.offset(), recordMetadata.partition());
//                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    private static StockQuote getTickerFeed(int symbolIndex) {
        float volume = random.nextFloat();
        float price = random.nextFloat() * (priceHighRange.get(symbolIndex).floatValue() - priceLowRange.get(symbolIndex).floatValue())
                + priceLowRange.get(symbolIndex).floatValue();
        float highPrice = priceHigh.get(symbolIndex).floatValue() > price ? priceHigh.get(symbolIndex).floatValue() : price;
        float lowPrice = priceLow.get(symbolIndex).floatValue() > price ? priceLow.get(symbolIndex).floatValue() : price;
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        return new StockQuote(
                symbols.get(symbolIndex).toString(),
                price,
                volume,
                highPrice,
                lowPrice,
                timestamp.toString()
                );
    }


    public static Producer<String, StockQuote> createStream() {
        setupSymbolLookup();
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, StockPriceConstants.BOOTSTRAP_SERVERS);
        prop.put(ProducerConfig.CLIENT_ID_CONFIG, StockPriceConstants.CLIENT_ID_CONFIG);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StockQuoteSerializer.class.getName());
        return new KafkaProducer<>(prop);
    }

    private static void setupSymbolLookup() {
        // set symbols
        symbols.put(0, "MSFT");
        symbols.put(1, "APPL");
        symbols.put(2, "MMT");
        symbols.put(3, "SBIN");
        symbols.put(4, "AMZN");
        symbols.put(5, "PNB");

        // set low/high price range
        priceLowRange.put(0, 90.0F);
        priceHighRange.put(0, 120.0F);
        priceLowRange.put(1, 170.0F);
        priceHighRange.put(1, 250.0F);
        priceLowRange.put(2, 4.0F);
        priceHighRange.put(2, 10.0F);
        priceLowRange.put(3, 250.0F);
        priceHighRange.put(3, 300.0F);
        priceLowRange.put(4, 1400.0F);
        priceHighRange.put(4, 1600.0F);
        priceLowRange.put(5, 90.0F);
        priceHighRange.put(5, 130.0F);

        // set volume
        lastVolume.put(0, 0.0F);
        lastVolume.put(1, 0.0F);
        lastVolume.put(2, 0.0F);
        lastVolume.put(3, 0.0F);
        lastVolume.put(4, 0.0F);
        lastVolume.put(5, 0.0F);

        // set high/low
        priceHigh.put(0, 0.0F);
        priceLow.put(0, 0.0F);
        priceHigh.put(1, 0.0F);
        priceLow.put(1, 0.0F);
        priceHigh.put(2, 0.0F);
        priceLow.put(2, 0.0F);
        priceHigh.put(3, 0.0F);
        priceLow.put(3, 0.0F);
        priceHigh.put(4, 0.0F);
        priceLow.put(4, 0.0F);
        priceHigh.put(5, 0.0F);
        priceLow.put(5, 0.0F);
    }
}
