package com.moqi.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Consumer Demo
 *
 * @author moqi
 * On 9/21/20 15:50
 */
@Slf4j
public class Consumer01 {

    private static final Map<TopicPartition, OffsetAndMetadata> CURRENT_OFFSETS = new HashMap<>();
    private static int COUNT = 0;

    public static void main(String[] args) {
        userDefineSubscribe();
    }

    public static KafkaConsumer<String, String> getConsumer() {
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
            try {
                // 同步提交
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }

    /**
     * 我们决定每处理 10 条记录就提交一次偏移量。在实际应用中，你可以根据时间或记录的内容进行提交。
     */
    private static void userDefineSubscribe() {
        KafkaConsumer<String, String> consumer = getConsumer();
        consumer.subscribe(Collections.singletonList("test"));

        try {
            //noinspection InfiniteLoopStatement
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100L));
                records.forEach(x -> {
                            log.info("topic:{}, partition={}, offset={}, customer={}, value={}",
                                    x.topic(), x.partition(), x.offset(), x.key(), x.value());

                            CURRENT_OFFSETS.put(new TopicPartition(x.topic(), x.partition()),
                                    new OffsetAndMetadata(x.offset() + 1, "No metadata"));

                            if (COUNT % 10 == 0) {
                                log.info("十条消息提交一次 Commit");
                                consumer.commitAsync(CURRENT_OFFSETS, null);
                            }

                            COUNT++;
                        }
                );

            }
        } finally {
            consumer.close();
        }
    }

}
