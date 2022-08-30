package de.hstr.bigdata;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Simple_Util implements AutoCloseable {

    private final Logger logger = LoggerFactory.getLogger(Util.class);
    private ExecutorService executorService = Executors.newFixedThreadPool(1);


    private Properties props;
    private String topic;
    private Producer<String, String> producer;
    private boolean closed;


    public void createTopics(final Properties allProps, List<NewTopic> topics)
            throws InterruptedException, ExecutionException, TimeoutException {
        try (final AdminClient client = AdminClient.create(allProps)) {
            System.err.println("Creating topics");

            client.createTopics(topics).values().forEach((topic, future) -> {
                try {
                    future.get();
                } catch (Exception ex) {
                    System.err.println(ex.toString());
                }
            });

            Collection<String> topicNames = topics
                    .stream()
                    .map(t -> t.name())
                    .collect(Collectors.toCollection(LinkedList::new));

            System.err.println("Asking cluster for topic descriptions");
            client
                    .describeTopics(topicNames)
                    .allTopicNames()
                    .get(10, TimeUnit.SECONDS)
                    .forEach((name, description) -> System.err.println("Topic Description: {}" + description.toString()));
        }
    }

    public void close() {
        //if (executorService != null) {
          //  executorService.shutdownNow();
          //  executorService = null;
       // }
    }
}