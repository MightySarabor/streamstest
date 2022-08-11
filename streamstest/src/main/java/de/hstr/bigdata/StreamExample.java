package de.hstr.bigdata;

import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyDescription.Subtopology;
import org.apache.kafka.streams.kstream.KStream;

/**
export KAFKA_OPTS="-Djava.security.auth.login.config=$PWD/kafka.jaas"
 
/usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh \ 
    --broker-list infbdt09:6667,infbdt10:6667,infbdt11:6667 \
    --topic bigdata075-1 \
    --producer-property security.protocol=SASL_PLAINTEXT
    
/usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh \
    --bootstrap-server infbdt06:6667,infbdt07:6667,infbdt08:6667 \
    --topic bigdata075-2 \
    --consumer-property security.protocol=SASL_PLAINTEXT 
 */
public class StreamExample {
    public static void main(String[] args) {

        // Achtung, Datei kafka.jaas muss angepasst werden!
        System.setProperty("java.security.auth.login.config", "/home/fleschm/kafka.jaas");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> lines = builder.stream("fleschm-1");
        KStream<String, String> transformed = lines
                .map((k, v) -> new KeyValue<String, String>(k, "Transformed in Kafka Streams: " + v));
        transformed.to("fleschm-2");

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "filter-warnings");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "infbdt07.fh-trier.de:6667,infbdt08.fh-trier.de:6667,infbdt09.fh-trier.de:6667");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        config.put("security.protocol", "SASL_PLAINTEXT");
        config.put("enable.auto.commit", "true");
        config.put("auto.commit.interval.ms", "1000");

        Topology topology = builder.build();

        TopologyDescription description = topology.describe();
        Subtopology subtopology = description.subtopologies().iterator().next();
        for (TopologyDescription.Node node : subtopology.nodes()) {
            System.err.println("-------------------");
            System.err.println(node.name());
            System.err.println(
                    node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList()));
        }

        @SuppressWarnings("resource")
        KafkaStreams streams = new KafkaStreams(topology, config);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}