package com.moqi.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author moqi
 * On 9/21/20 15:50
 */
@Slf4j
public class Producer01 {

    public static void main(String[] args) throws Exception {
        fireAndForget();
        syncSend();
        asyncSend();
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
        ProducerRecord<String, String> record = new ProducerRecord<>("test", "Precision Products", "fireAndForget");

        producer.send(record);
    }

    /**
     * 同步发送
     */
    private static void syncSend() throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> producer = getProducer();
        ProducerRecord<String, String> record = new ProducerRecord<>("test", "Precision Products", "syncSend");

        RecordMetadata recordMetadata = producer.send(record).get();
        long offset = recordMetadata.offset();
        int partition = recordMetadata.partition();
        log.info("offset:{}, partition:{}", offset, partition);
    }

    /**
     * 异步发送
     */
    private static void asyncSend() {
        KafkaProducer<String, String> producer = getProducer();
        ProducerRecord<String, String> record = new ProducerRecord<>("test", "Precision Products", "asyncSend");

        producer.send(record, new DemoProducerCallback());
    }

    private static class DemoProducerCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata metadata, Exception e) {
            if (Objects.nonNull(e)) {
                log.error("metadata:{}, e:{}", metadata, e);
            }
        }

    }
}
