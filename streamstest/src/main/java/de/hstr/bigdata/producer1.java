package de.hstr.bigdata;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


public class producer1 {
    public static void main(String args[])
    {
        //Producer properties
        String bootstrapServers= "localhost:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create producer
        KafkaProducer<String, String> first_producer = new KafkaProducer<String, String>(properties);

        //Create producerRecord
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("my_first", "Hye Kafka");

        //Invoke object of producerRecord
        //first_producer.send(record);

        first_producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                Logger logger=LoggerFactory.getLogger(producer1call.class);
                if (e== null) {
                    logger.info("Successfully received the details as: \n" +
                            "Topic:" + recordMetadata.topic() + "\n" +
                            "Partition:" + recordMetadata.partition() + "\n" +
                            "Offset" + recordMetadata.offset() + "\n" +
                            "Timestamp" + recordMetadata.timestamp());
                }

                else {
                    logger.error("Can't produce,getting error",e);

                }
            }
        });

        first_producer.flush();
        first_producer.close();

    }
}
