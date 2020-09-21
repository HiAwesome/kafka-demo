package com.moqi;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author moqi
 * On 9/21/20 15:50
 */
@Slf4j
public class Producer01 {

    public static void main(String[] args) throws Exception {
        syncSend();
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

    /**
     * 同步发送
     */
    private static void syncSend() throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> producer = getProducer();
        ProducerRecord<String, String> record = new ProducerRecord<>("test", "Precision Products", "France");

        RecordMetadata recordMetadata = producer.send(record).get();
        long offset = recordMetadata.offset();
        int partition = recordMetadata.partition();
        log.info("offset:{}, partition:{}", offset, partition);
    }

}
