package com.moqi.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 独立消费者
 * <p>
 * 到目前为止，我们讨论了消费者群组，分区被自动分配给群组里的消费者，在群组里新增或移除消费者时自动触发再均衡。
 * 通常情况下，这些行为刚好是你所需要的，不过有时候你需要一些更简单的东西。
 * 比如，你可能只需要一个消费者从一个主题的所有分区或者某个特定的分区读取数据。
 * 这个时候就不需要消费者群组和再均衡了，只需要把主题或者分区分配给消费者，然后开始读取消息并提交偏移量。
 * <p>
 * 如果是这样的话，就不需要订阅主题，取而代之的是为自己分配分区。
 * 一个消费者可以订阅主题（并加入消费者群组），
 * 或者为自己分配分区，但不能同时做这两件事情。
 *
 * @author moqi
 * On 9/22/20 15:30
 */
@Slf4j
public class IndependentConsumer {

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = Consumer01.getConsumer();
        List<PartitionInfo> partitionInfoList = consumer.partitionsFor("test");
        List<TopicPartition> partitions = new ArrayList<>();

        if (Objects.nonNull(partitionInfoList)) {
            partitionInfoList.forEach(x -> partitions.add(new TopicPartition(x.topic(), x.partition())));
            consumer.assign(partitions);

            try {
                //noinspection InfiniteLoopStatement
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

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

}
