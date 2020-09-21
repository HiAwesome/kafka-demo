package com.moqi.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Consumer Demo
 *
 * @author moqi
 * On 9/21/20 15:50
 */
@Slf4j
public class Consumer01 {

    public static void main(String[] args) {
        subscribe();
    }

    private static KafkaConsumer<String, String> getConsumer() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("group.id", "HelloWorld");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<>(kafkaProps);
    }

    private static void subscribe() {
        KafkaConsumer<String, String> consumer = getConsumer();
        consumer.subscribe(Collections.singletonList("test"));

        try {
            //noinspection InfiniteLoopStatement
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100L));
                records.forEach(x -> log.info("topic:{}, partition={}, offset={}, customer={}, value={}",
                        x.topic(), x.partition(), x.offset(), x.key(), x.value()));

                // 异步提交
                consumer.commitAsync();
            }
        } finally {
            consumer.close();
        }
    }

}
