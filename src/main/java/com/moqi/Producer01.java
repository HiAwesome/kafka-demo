package com.moqi;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * @author moqi
 * On 9/21/20 15:50
 */

public class Producer01 {

    public static void main(String[] args) {

    }

    public static void getProducer() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);
    }

}
