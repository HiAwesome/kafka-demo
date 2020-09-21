package com.moqi.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 自定义分区器
 *
 * @author moqi
 * On 9/21/20 20:43
 */

public class BananaPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        if (Objects.isNull(keyBytes) || !(key instanceof String)) {
            throw new InvalidRecordException("We expect all messages to have customer name as key");
        }

        if (String.valueOf(key).equals("Banana")) {
            // Banana 总是被分配到最后一个分区
            return numPartitions;
        } else {
            // 其他记录被散列到其他分区
            return Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
