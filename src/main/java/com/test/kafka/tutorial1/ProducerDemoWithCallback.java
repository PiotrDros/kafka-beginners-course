package com.test.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);


        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {
            sendMessage(logger, producer, i);
        }


        // flush and close producer
        producer.close();
    }

    private static void sendMessage(final Logger logger, KafkaProducer<String, String> producer, int i) {


        // create a producer record
        String first_topic = "first_topic";
        String value = "Hello World!! " + i;

        ProducerRecord<String,String> record = new ProducerRecord<String, String>(first_topic, value);

        // send data asynchronously
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    logger.info("Received new metadada" + metadata);
                    System.out.println("Received new metadada" + metadata);

                } else {
                    logger.error("Error has occured", exception);
                    System.out.println(("Error has occured" + exception));

                }


            }
        });
    }
}
