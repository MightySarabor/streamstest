package de.hstr.bigdata;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Arrays;

import java.util.Properties;
import java.util.stream.Collectors;

public class LineSplit {

    public static void main(String[] args) {

        // Achtung, Datei kafka.jaas muss angepasst werden!
        System.setProperty("java.security.auth.login.config", "/home/fleschm/kafka.jaas");

        final StreamsBuilder builder = new StreamsBuilder();

        builder.<String, String>stream("fleschm-1")
                .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
                .to("fleschm-2");


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
        TopologyDescription.Subtopology subtopology = description.subtopologies().iterator().next();
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
