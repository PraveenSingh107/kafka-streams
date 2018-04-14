package com.praveen.kafka.frauddetection;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.serialization.Serdes.serdeFrom;

public class POSTransactionProcessor {

    private static final Logger logger = LoggerFactory.getLogger(POSTransactionProcessor.class);

    public static void main(String[] args) {
        start();
    }

    public static void start() {
        KafkaStreams streams = createStream();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static KafkaStreams createStream() {

        Serde<UserRecord> userRecordSerde = serdeFrom(new UserRecordSerializer(), new UserRecordDeserializer());
        Serde<UserHistory> userHistorySerde = serdeFrom(new UserHistorySerializer(), new UserHistoryDeserializer());
        Serde<Aggregated> trnxStatsSerdes = serdeFrom(new TrnxStatsSerializer(), new TrnxStatsDeserializer());

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, Constants.FRAUD_DETECTION_APPLICATION_ID_CONFIG);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.FRAUD_DETECTION_BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, POSTransactionStatSerdes.class.getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.STATE_DIR_CONFIG, Constants.FRAUD_DETECTION_STATE_STORE_DIR);

        Serde<Transaction> transactionSerde = serdeFrom(new TransactionSerializer(),
                new TransactionDeserializer());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Transaction> transactionS = builder.stream(Constants.POS_TRANSACTIONS_TOPIC,
                Consumed.with(Serdes.String(), transactionSerde));

        KTable<String, UserRecord> usrRecordsT = builder.table(Constants.USER_PROFILE_TOPIC,
                Consumed.with(Serdes.String(), userRecordSerde));



        KStream<String, EnrichedTransaction> enrichedTrnxS = transactionS
                .leftJoin(usrRecordsT,
                (transaction, user) -> new EnrichedTransaction(user, transaction));


        KTable<String, UserHistory> historyT = builder.table(Constants.USER_TRANSACTION_BEHAVIOR_TOPIC,
                Consumed.with(Serdes.String(), userHistorySerde));


        TimeWindowedKStream<String, EnrichedTransaction> enrichedTrnxWS = enrichedTrnxS.
                        groupByKey().
                        windowedBy(TimeWindows.of(TimeUnit.HOURS.toMillis(1)));





        KTable<Windowed<String>, Aggregated> aggregatedWKTable = enrichedTrnxWS.
                aggregate(Aggregated::new,
                (cardNumber, transaction, ts) -> ts.add(transaction),
                Materialized.<String, Aggregated,
                        WindowStore<Bytes, byte[]>>as("1hour-aggregated-store").withValueSerde(trnxStatsSerdes));


        KStream<String, Transaction> locFraudTransactions = aggregatedWKTable.filter((cn, aggregated) -> {
            return FraudLocationEstimator.getFraudLocationIndex(aggregated.transactions) >= 5.0F;
        }).toStream().flatMap(((cn, tStats) -> {
            List<KeyValue<String, Transaction>> transactions = new ArrayList<>();
            tStats.transactions.forEach(enrichedTransaction -> {
                transactions.add(KeyValue.pair(cn.key(), enrichedTransaction.getTransaction()));
            });
            return transactions;
        }));

        KStream<String, Aggregated> transformedStream = aggregatedWKTable.toStream().
                map((cardNum, tStats) -> KeyValue.pair(cardNum.key(), tStats));

        KStream<String, CombinedData> highTransactionsS = transformedStream.
                join(historyT, (aggregated, historyData) -> new CombinedData(aggregated, historyData)).
                filter((cardNumber, combinedData) ->
                        combinedData.Aggregated.getAvgAmount() > 10 * combinedData.userRecord.getAvgTrnxAmount());

        KStream<String, Transaction> amountFraudTransactionS = highTransactionsS.
                flatMap(((cardNumber, combinedData) -> {
                    List<KeyValue<String, Transaction>> transactions = new ArrayList<>();
                    combinedData.Aggregated.transactions.forEach(transaction -> {
                        transactions.add(KeyValue.pair(cardNumber, transaction.getTransaction()));
                    });
                    return transactions;
                }));

        KStream<String, Transaction> fraudTrnxStream = amountFraudTransactionS.
                outerJoin(locFraudTransactions, (amountFraudTrnx, locFraudTrnx) -> amountFraudTrnx,
                        JoinWindows.of(TimeUnit.SECONDS.toMillis(30)),
                        Joined.with(Serdes.String(), transactionSerde, transactionSerde));


        fraudTrnxStream.to(Constants.FRAUD_TRANSACTIONS_TOPIC,
                Produced.with(Serdes.String(), transactionSerde));



        return new KafkaStreams(builder.build(), props);

    }
}

class CombinedData {
    Aggregated Aggregated;
    UserHistory userRecord;

    public CombinedData(Aggregated Aggregated, UserHistory userRecord) {
        this.Aggregated = Aggregated;
        this.userRecord = userRecord;
    }
}

