package com.moqi.serializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * AvroDeserializer
 *
 * @author moqi
 * On 9/21/20 17:05
 */
@Slf4j
public class AvroDeserializer {

    public static void main(String[] args) {
        KafkaConsumer<String, GenericRecord> consumer = getConsumer();
        consumer.subscribe(Collections.singletonList("customer"));

        //noinspection InfiniteLoopStatement
        while (true) {
            ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
            records.forEach(x -> {
                GenericRecord record = x.value();
                log.info("id:{}, name:{}, email:{}", record.get("id"), record.get("name"), record.get("email"));
            });


        }
    }

    private static KafkaConsumer<String, GenericRecord> getConsumer() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("group.id", "HelloWorld");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        kafkaProps.put("schema.registry.url", "http://localhost:8081");

        return new KafkaConsumer<>(kafkaProps);
    }

}
