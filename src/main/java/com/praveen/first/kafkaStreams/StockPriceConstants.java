package com.praveen.first.kafkaStreams;

public class StockPriceConstants {
    public static final String STOCK_PRICE_FEED_TOPIC = "stock-price-feed";
    public static final String STOCK_PRICE_FEED_PROCESSING_TOPIC = "stock-price-feed-processing";
    public static final String STOCK_LIQUIDITY_TOPIC = "stock-liquidity-processing";
    public static final String BOOTSTRAP_SERVERS = "kafka-cluster:9092";
    public static final String CLIENT_ID_CONFIG = "stock-quote-processing";
}
