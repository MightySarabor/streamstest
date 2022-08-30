package de.hstr.bigdata;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.common.serialization.Serdes;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCountWithUtil {

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

    static Topology buildTopology(String inputTopic, String outputTopic) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic).
                peek((k,v) -> System.err.printf("Observed Event:" + v + "\n")).
                to(outputTopic);
        final Topology topology = builder.build();
        System.err.println(topology.describe());
        return topology;
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: the path to a configuration file.");
        }
        Properties props = new Properties();
        try (InputStream inputStream = new FileInputStream(args[0])) {
            props.load(inputStream);
        }
        //Topic Namen zu weisen
        final String inputTopic = props.getProperty("input.topic.name");
        final String outputTopic = props.getProperty("output.topic.name");
        final String partitions = props.getProperty("num.partitions");
        final String replication = props.getProperty("num.replication");
        //Topics erstellen (falls noch keine da sind)
        try (Simple_Util utility = new Simple_Util()) {

            utility.createTopics(
                    props,
                    Arrays.asList(
                            new NewTopic(inputTopic, Integer.parseInt(partitions), (short) Integer.parseInt(replication)),
                            new NewTopic(outputTopic, Integer.parseInt(partitions), (short) Integer.parseInt(replication))));

            try{
                System.err.println("Building topology.");
                KafkaStreams kafkaStreams = new KafkaStreams(
                        buildTopology(inputTopic, outputTopic),
                        props);

                Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

                System.err.println("Starting Kafka Streams.");
                runKafkaStreams(kafkaStreams);
                System.err.println("Shutting down Kafka Streams.");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }



        }
    }
}
