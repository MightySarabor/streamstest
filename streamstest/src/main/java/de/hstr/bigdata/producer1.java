package de.hstr.bigdata;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class producer1 {
    public static void main(String args[]) throws ExecutionException, InterruptedException {
        //Producer properties
        String bootstrapServers= "localhost:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create producer with specific key
        KafkaProducer<String, String> first_producer = new KafkaProducer<String, String>(properties);
            for(int i = 0; i < 10; i++) {
                String topic = "my_first";
                String value = "OneTwo" + Integer.toString(i);
                String key = "id_" + Integer.toString(i);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
                System.err.println(("key: " + key));

                //Create producerRecord
                //ProducerRecord<String, String> record = new ProducerRecord<String, String>("my_first", "Hye Kafka");


                //Invoke object of producerRecord
                //first_producer.send(record);

                first_producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        Logger logger = LoggerFactory.getLogger(producer1.class);
                        if (e == null) {
                            System.err.println("Successfully received the details as: \n" +
                                    "Topic:" + recordMetadata.topic() + "\n" +
                                    "Partition:" + recordMetadata.partition() + "\n" +
                                    "Offset" + recordMetadata.offset() + "\n" +
                                    "Timestamp" + recordMetadata.timestamp());
                        } else {
                            System.err.println("Can't produce,getting error" + e);

                        }
                    }
                }).get();//sending synchronous data forcefully
            }
        first_producer.flush();
        first_producer.close();

    }
}
