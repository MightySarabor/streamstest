package de.hstr.bigdata;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class WordCount {
    public static void main(String[] args) {

        // Achtung, Datei kafka.jaas muss angepasst werden!
        System.setProperty("java.security.auth.login.config", "/home/fleschm/kafka.jaas");

        //Eigentlicher Code
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream("fleschm-1");
        source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
                .groupBy((key, value) -> value)
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
                .toStream()
                .to("fleschm-2", Produced.with(Serdes.String(), Serdes.Long()));
        // Code zu Ende
        //Propoerties
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "fleschm-wordcount");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "infbdt07.fh-trier.de:6667,infbdt08.fh-trier.de:6667,infbdt09.fh-trier.de:6667");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        config.put("security.protocol", "SASL_PLAINTEXT");
        //Properties zu Ende

        //Topology beschreiben
        final Topology topology = builder.build();
        System.out.println(topology.describe());
        final KafkaStreams streams = new KafkaStreams(topology, config);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}

