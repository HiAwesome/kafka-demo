package com.moqi.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * Producer Demo
 * <p>
 * bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test
 *
 * @author moqi
 * On 9/21/20 15:50
 */
@Slf4j
public class Producer01 {

    private static final Random RANDOM = new Random();

    public static void main(String[] args) throws Exception {
        //noinspection InfiniteLoopStatement
        while (true) {
            //noinspection BusyWait
            Thread.sleep(RANDOM.nextInt(1000));
            fireAndForget();
            syncSend();
            asyncSend();
        }
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
        ProducerRecord<String, String> record = new ProducerRecord<>("test", "asyncSend" + RANDOM.nextInt(10000));

        producer.send(record);
    }

    /**
     * 同步发送
     */
    private static void syncSend() throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> producer = getProducer();
        ProducerRecord<String, String> record = new ProducerRecord<>("test", "asyncSend" + RANDOM.nextInt(10000));

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
        ProducerRecord<String, String> record = new ProducerRecord<>("test", "asyncSend" + RANDOM.nextInt(10000));

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
