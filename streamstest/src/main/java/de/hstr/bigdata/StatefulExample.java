package de.hstr.bigdata;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

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

    private static KafkaStreams streams;

    public static void main(String[] args) {
        // Achtung, Datei kafka.jaas muss angepasst werden!
        System.setProperty("java.security.auth.login.config", "/home/fleschm/kafka.jaas");

        StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> textLines = builder.stream("fleschm-1");
        KTable<String, Long> count = textLines.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .peek((k, v) -> System.err.printf("Word: (%s, %s)\n", k, v))
                .groupBy((key, value) -> value)
                .count(Materialized.as("fleschm-store"));

        count.toStream()
                .peek((key, value) -> System.err.printf("Outgoing record (%s, %s)\n", key, value))
                .to("fleschm-2", Produced.with(Serdes.String(), Serdes.Long()));

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "fleschm-test");
        config.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 2);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "infbdt07.fh-trier.de:6667,infbdt08.fh-trier.de:6667,infbdt09.fh-trier.de:6667");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        config.put("security.protocol", "SASL_PLAINTEXT");
        config.put("enable.auto.commit", "true");
        config.put("auto.commit.interval.ms", "100");

        System.err.println("<--- Stateful Example --->");
        System.err.println("Building topology.");
        Topology topology = builder.build();

        streams = new KafkaStreams(topology, config);

        System.err.println("Starting Kafka Streams.");
        Util utility = new Util();
        Util.Randomizer rando = utility.startNewRandomizer(config, "fleschm-1");
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(StatefulExample::shutdown));
    }

    public static void shutdown() {
        if (streams != null) {
            System.err.println("Shutting down Kafka Streams.");
            streams.close();
        }
    }
}