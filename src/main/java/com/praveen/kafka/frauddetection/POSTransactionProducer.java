package com.praveen.kafka.frauddetection;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class POSTransactionProducer {

    private static final Logger logger = LoggerFactory.getLogger(POSTransactionProducer.class);

    public static void produce() {
        produceUserDetails();
        producePOSTransactionData();

        produceUserTransactionBehaviourData();
    }

    private static void producePOSTransactionData() {
        Producer<String, Transaction> producer = createProducer();
        List<Transaction> transactions = getPOSTransactions();
        for (Transaction transaction : transactions) {
            ProducerRecord<String, Transaction> record =
                    new ProducerRecord<>(
                            Constants.POS_TRANSACTIONS_TOPIC,
                            transaction.getCardNumber(),
                            transaction
                    );
            RecordMetadata recordMetadata = null;
            try {
                recordMetadata = producer.send(record).get();
                logger.info("POS transaction sent: key={}, value={}, offset={}, partition={}", record.key(),
                        record.value(), recordMetadata.offset(), recordMetadata.partition());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    private static void produceUserDetails() {
        Producer<String, UserRecord> producer = createUserRecordProducer();
        List<UserRecord> userDetails = getUserDetails();
        for (UserRecord userRecord : userDetails) {
            ProducerRecord<String, UserRecord> record =
                    new ProducerRecord<>(
                            Constants.USER_PROFILE_TOPIC,
                            userRecord.getCardNumber(),
                            userRecord
                    );
            RecordMetadata recordMetadata = null;
            try {
                recordMetadata = producer.send(record).get();
                logger.info("userrecords sent: key={}, value={}, offset={}, partition={}", record.key(),
                        record.value(), recordMetadata.offset(), recordMetadata.partition());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    private static void produceUserTransactionBehaviourData() {
        Producer<String, UserHistory> producer = createUserTransactionBehaviorDataProducer();
        List<UserHistory> userHistoryDetails = getUserTransactionBehaviorDetails();
        for (UserHistory userHistory : userHistoryDetails) {
            ProducerRecord<String, UserHistory> record =
                    new ProducerRecord<>(
                            Constants.USER_TRANSACTION_BEHAVIOR_TOPIC,
                            userHistory.getCardNumber(),
                            userHistory
                    );
            RecordMetadata recordMetadata = null;
            try {
                recordMetadata = producer.send(record).get();
                logger.info("user transaction behavior record sent: key={}, value={}, offset={}, partition={}", record.key(),
                        record.value(), recordMetadata.offset(), recordMetadata.partition());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    private static List<UserRecord> getUserDetails() {
        return Arrays.asList(
                new UserRecord(1, "Charlie", "smith", "charlie@asdf.com", "India", "5432154321"),
                new UserRecord(2, "Alice", "smith", "alice@asdf.com", "India", "1234512345"),
                new UserRecord(3, "Bob", "smith", "bob@asdf.com", "India", "9876598765")
        );
    }

    private static List<UserHistory> getUserTransactionBehaviorDetails() {
        return Arrays.asList(
                new UserHistory(1, "1234512345", 50000.0F, 100000.0F, 3),
                new UserHistory(2, "5432154321", 40000.0F, 80000.0F, 3),
                new UserHistory(3, "9876598765", 150000.0F, 200000.0F, 3)
        );
    }

    private static List<Transaction> getPOSTransactions() {
        Date date = new Date();
        return Arrays.asList(

                new Transaction(1, "101", "1234512345",
                        new Timestamp(date.getTime()), 15000.6F,
                        "retail", "zone1"),

                new Transaction(2, "102", "1234512345",
                        new Timestamp(date.getTime() + 20 * 1000), 75200.6F,
                        "retail", "zone2"),

                new Transaction(3, "103", "1234512345",
                        new Timestamp(date.getTime() + 30 * 1000), 1345000.6F,
                        "retail", "zone3"),

                new Transaction(4, "104", "1234512345",
                        new Timestamp(date.getTime() + 40 * 1000), 14565000.6F,
                        "retail", "zone4"),

                new Transaction(5, "105", "1234512345",
                        new Timestamp(date.getTime() + 50 * 1000), 1235000.6F,
                        "retail", "zone5"),

                /*==============================================================================================*/

                new Transaction(1, "201", "5432154321",
                        new Timestamp(date.getTime() + 20 * 1000), 15000.6F,
                        "e-commerce", "zone1"),

                new Transaction(2, "202", "5432154321",
                        new Timestamp(date.getTime() + 3 * 20000), 5000.6F,
                        "e-commerce", "zone1"),

                new Transaction(3, "203", "5432154321",
                        new Timestamp(date.getTime() + 4 * 20000), 14000.6F,
                        "e-commerce", "zone1"),

                new Transaction(4, "204", "5432154321",
                        new Timestamp(date.getTime() + 6 * 20000), 20000.6F,
                        "e-commerce", "zone1"),

                new Transaction(5, "205", "5432154321",
                        new Timestamp(date.getTime() + 8 * 20000), 34000.6F,
                        "e-commerce", "zone1"),

                /*==============================================================================================*/

                new Transaction(1, "301", "9876598765",
                        new Timestamp(date.getTime() + 3 * 20000), 50000000.6F,
                        "e-commerce", "zone1"),

                new Transaction(1, "302", "9876598765",
                        new Timestamp(date.getTime() + 6 * 20000), 65500000.6F,
                        "e-commerce", "zone1"),

                new Transaction(1, "303", "9876598765",
                        new Timestamp(date.getTime() + 9 * 20000), 6785000.6F,
                        "e-commerce", "zone1"),

                new Transaction(1, "304", "9876598765",
                        new Timestamp(date.getTime() + 12 * 20000), 2475000.6F,
                        "e-commerce", "zone1"),

                new Transaction(1, "305", "9876598765",
                        new Timestamp(date.getTime() + 15 * 20000), 9865000.6F,
                        "e-commerce", "zone1")
        );
    }

    private static KafkaProducer<String, Transaction> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.FRAUD_DETECTION_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, Constants.FRAUD_DETECTION_CLIENT_ID_CONFIG);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TransactionSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private static KafkaProducer<String, UserRecord> createUserRecordProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.FRAUD_DETECTION_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, Constants.FRAUD_DETECTION_CLIENT_ID_CONFIG);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserRecordSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private static KafkaProducer<String, UserHistory> createUserTransactionBehaviorDataProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.FRAUD_DETECTION_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, Constants.FRAUD_DETECTION_CLIENT_ID_CONFIG);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserHistorySerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}
