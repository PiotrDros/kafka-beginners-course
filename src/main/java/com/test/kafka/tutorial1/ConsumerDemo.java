package com.test.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemo {
    public static void main(String[] args) throws  Exception{
        new ConsumerDemo().run();

    }

    public void run() throws Exception {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-fourth-application";
        String topic = "first_topic";

        CountDownLatch latch = new CountDownLatch(1);

        ConsumerRunnable consumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, topic, latch);

        // start the therad
        new Thread(consumerRunnable).start();


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          consumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        latch.await();


    }

    public class ConsumerRunnable implements Runnable {
        private CountDownLatch latch;
        // create consumer
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String bootstrapServers, String groupId, String topic, CountDownLatch latch) {
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


            this.latch = latch;
            this.consumer = new KafkaConsumer<String, String>(properties);

            // subscribe consumer to a topic
            consumer.subscribe(Collections.singleton(topic));
        }

        public void shutdown() {
            // method to interrupt consumer.poll()
            consumer.wakeup();
        }


        @Override
        public void run() {
            try {


                // poll for new data
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println("Key: " + record.key() + ", value: " + record.value());
                    }


                }

            } catch (WakeupException e) {
                System.out.println("Recived shut down singal!");
            } finally {
                consumer.close();
                // tell our main coce we're done with the consumer
                latch.countDown();
            }

        }
    }


}
