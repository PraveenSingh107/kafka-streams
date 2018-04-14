package com.anomaly_detection_stream;

import com.anomaly_detection_stream.model.UserClick;
import com.anomaly_detection_stream.serializer.UserClickDeserializer;
import com.anomaly_detection_stream.serializer.UserClickSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.lang.String;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.serialization.Serdes.String;
import static org.apache.kafka.common.serialization.Serdes.*;
import static org.apache.kafka.streams.StreamsConfig.*;

public class AnomalyDetectionStreamApp {


	private static final String BOOTSTRAP_SERVERS = "kafka-cluster:9092";

	public static void main(String[] args) {
		start();
	}

	public static void start() {
		KafkaStreams streams = createStream();
		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}


	public static KafkaStreams createStream() {
		Serde<UserClick> userClickSerdes = serdeFrom(new UserClickSerializer(), new UserClickDeserializer());

		Properties props = new Properties();
		props.put(APPLICATION_ID_CONFIG, "anomaly-detection-stream");
		props.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class.getName());
		props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class.getName());
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "my-state-store");

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, UserClick> stream = builder.stream(Topics.USER_CLICK_TOPIC, Consumed.with(
				Serdes.String(),
				userClickSerdes));
        KStream<String, UserClick> mappedStream = stream.map((key, userClick) -> new KeyValue<>(userClick.getUser(), userClick));
        TimeWindowedKStream<String, UserClick> stringUserClickTimeWindowedKStream = mappedStream.groupBy((key, value) -> key).
                windowedBy(TimeWindows.of(10000));
        KStream<String, Long> keyValueLongKStream = stringUserClickTimeWindowedKStream.count().toStream((key, value) -> key.key());
        keyValueLongKStream.print(Printed.toSysOut());
        keyValueLongKStream.to(Topics.CLICK_COUNT, Produced.with(Serdes.String(), Serdes.Long()));
        return new KafkaStreams(builder.build(), props);
	}
}
