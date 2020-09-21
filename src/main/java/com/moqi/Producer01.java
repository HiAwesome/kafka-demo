package com.moqi;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author moqi
 * On 9/21/20 15:50
 */

public class Producer01 {

    public static void main(String[] args) {
        fireAndForget();
    }

    private static KafkaProducer<String, String> getProducer() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(kafkaProps);
    }

    /**
     * 发送并忘记
     */
    private static void fireAndForget() {
        KafkaProducer<String, String> producer = getProducer();
        ProducerRecord<String, String> record = new ProducerRecord<>("test", "Precision Products", "France");

        producer.send(record);
    }

}
