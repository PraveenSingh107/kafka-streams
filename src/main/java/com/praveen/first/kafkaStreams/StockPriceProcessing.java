package com.praveen.first.kafkaStreams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.util.Properties;

import static org.apache.kafka.common.serialization.Serdes.serdeFrom;

public class StockPriceProcessing {

    public static void main(String[] args) {
        start();
    }

    public static void start() {
        KafkaStreams streams = createStream();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static KafkaStreams createStream() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Stock-price-movement-notifier");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, StockPriceConstants.BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.FloatSerde.class.getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "my-stock-quotes-state-store");

        Serde<StockQuote> stockQuoteSerde = serdeFrom(new StockQuoteSerializer(), new StockQuoteDeserializer());
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, StockQuote> stream = builder.stream(
                StockPriceConstants.STOCK_PRICE_FEED_TOPIC,
                Consumed.with(Serdes.String(), stockQuoteSerde));

        KStream<String, StockQuote> appleStream = stream.filter((symbol, stockQuote) -> symbol.toUpperCase().equals("APPL"));
        appleStream.print(Printed.toSysOut());
        KTable<String, Float> aggregatedTable = appleStream.groupByKey().aggregate(() -> 0.0F,
                ((aggKey, newVal, aggVal) -> newVal.getVolume() + aggVal.floatValue()), Materialized.as("total-volume"));
        aggregatedTable.toStream().print(Printed.toSysOut());

//
//        KTable<Windowed<String>, Long> applLiquidityCount = stream.filter((symbol, stockQuote) -> symbol.toUpperCase().equals("APPL")).
//                groupByKey().count(TimeWindows.of(40 * 1000L), "appl-liquidity");
//
//        KTable<Windowed<String>, Long> msftLiquidityCount = stream.filter((symbol, stockQuote) -> symbol.toUpperCase().equals("MSFT")).
//                groupByKey().count(TimeWindows.of(40 * 1000L), "msft-liquidity");
//
//        KTable<Windowed<String>, Long> mmtLiquidityCount = stream.filter((symbol, stockQuote) -> symbol.toUpperCase().equals("MMT")).
//                groupByKey().count(TimeWindows.of(40 * 1000L), "mmt-liquidity");
//
//        KTable<Windowed<String>, Long> pngLiquidityCount = stream.filter((symbol, stockQuote) -> symbol.toUpperCase().equals("PNB")).
//                groupByKey().count(TimeWindows.of(40 * 1000L), "pnb-liquidity");
//
//        KTable<Windowed<String>, Long> sbinLiquidityCount = stream.filter((symbol, stockQuote) -> symbol.toUpperCase().equals("SBIN")).
//                groupByKey().count(TimeWindows.of(40 * 1000L), "sbin-liquidity");
//
//        KStream<String, Long> applyLiquidityCount = applLiquidityCount.
//                toStream().
//                filter((windowedSymbol, count) -> count != null).
//                map((windowedSymbol, count) -> new KeyValue<>(windowedSymbol.key(), count));
//        applyLiquidityCount.print(Printed.toSysOut());
//
//        KStream<String, Long> msftLiquidityCountStream = msftLiquidityCount.
//                toStream().
//                filter((windowedSymbol, count) -> count != null).
//                map((windowedSymbol, count) -> new KeyValue<>(windowedSymbol.key(), count));
//        msftLiquidityCountStream.print(Printed.toSysOut());
//
//        KStream<String, Long> mmtLiquidityCountStream = mmtLiquidityCount.
//                toStream().
//                filter((windowedSymbol, count) -> count != null).
//                map((windowedSymbol, count) -> new KeyValue<>(windowedSymbol.key(), count));
//        mmtLiquidityCountStream.print(Printed.toSysOut());
//
//        KStream<String, Long> pnbLiquidityCountStream = pngLiquidityCount.
//                toStream().
//                filter((windowedSymbol, count) -> count != null).
//                map((windowedSymbol, count) -> new KeyValue<>(windowedSymbol.key(), count));
//        pnbLiquidityCountStream.print(Printed.toSysOut());
//
//        KStream<String, Long> sbinLiquidityCountStream = sbinLiquidityCount.
//                toStream().
//                filter((windowedSymbol, count) -> count != null).
//                map((windowedSymbol, count) -> new KeyValue<>(windowedSymbol.key(), count));
//        sbinLiquidityCountStream.print(Printed.toSysOut());
//
//        applyLiquidityCount.to(StockPriceConstants.STOCK_LIQUIDITY_TOPIC);
//        msftLiquidityCountStream.to(StockPriceConstants.STOCK_LIQUIDITY_TOPIC);
//        mmtLiquidityCountStream.to(StockPriceConstants.STOCK_LIQUIDITY_TOPIC);
//        pnbLiquidityCountStream.to(StockPriceConstants.STOCK_LIQUIDITY_TOPIC);
//        sbinLiquidityCountStream.to(StockPriceConstants.STOCK_LIQUIDITY_TOPIC);

        return new KafkaStreams(builder.build(), props);
    }
}
