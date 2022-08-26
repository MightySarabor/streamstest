package de.hstr.bigdata;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;


/**
 * export KAFKA_OPTS="-Djava.security.auth.login.config=$PWD/kafka.jaas"
 *
 * /usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh \ --broker-list
 * infbdt09:6667,infbdt10:6667,infbdt11:6667 \ --topic fleschm-1 \
 * --producer-property security.protocol=SASL_PLAINTEXT
 *
 * /usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh \
 * --bootstrap-server infbdt06:6667,infbdt07:6667,infbdt08:6667 \ --topic
 * fleschm-2 \ --consumer-property security.protocol=SASL_PLAINTEXT
 */
public class StatefulExample {

    static void runKafkaStreams(final KafkaStreams streams) {
        final CountDownLatch latch = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            if (oldState == KafkaStreams.State.RUNNING && newState != KafkaStreams.State.RUNNING) {
                latch.countDown();
            }
        });

        streams.start();

        try {
            latch.await();
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }

        System.err.printf("Streams Closed");
    }
    private static KafkaStreams streams;
    static Topology buildTopology(String inputTopic, String outputTopic) {
        System.setProperty("java.security.auth.login.config", "/home/fleschm/kafka.jaas");
        StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> textLines = builder.stream(inputTopic);
        KTable<String, Long> count = textLines.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .peek((k, v) -> System.err.printf("Word: (%s, %s)\n", k, v))
                .groupBy((key, value) -> value)
                .count(Materialized.as("fleschm-store"));

        count.toStream()
                .peek((key, value) -> System.err.printf("Outgoing record (%s, %s)\n", key, value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        Properties config = new Properties();
        return builder.build();
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: the path to a configuration file.");
        }
        Properties props = new Properties();
        try (InputStream inputStream = new FileInputStream(args[0])) {
            props.load(inputStream);
        }

        final String inputTopic = props.getProperty("input.topic.name");
        final String outputTopic = props.getProperty("output.topic.name");

        try (Util utility = new Util()) {

            utility.createTopics(
                    props,
                    Arrays.asList(
                            new NewTopic(inputTopic, Optional.empty(), Optional.empty()),
                            new NewTopic(outputTopic, Optional.empty(), Optional.empty())));

            // Ramdomizer only used to produce sample data for this application, not typical usage
            try (Util.Randomizer rando = utility.startNewRandomizer(props, inputTopic)) {
                System.err.println("Building topology.");
                KafkaStreams kafkaStreams = new KafkaStreams(
                        buildTopology(inputTopic, outputTopic),
                        props);

                Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

                System.err.println("Starting Kafka Streams.");

                runKafkaStreams(kafkaStreams);
                System.err.println("Shutting down Kafka Streams.");

            }
        }
    }
}