package com.moqi.serializer;

import com.moqi.bean.Customer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;


/**
 * 自定义序列化
 *
 * @author moqi
 * On 9/21/20 16:41
 */
public class CustomerSerializer implements Serializer<Customer> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // 不做任何配置
    }

    /**
     * Customer对象被序列化成：
     * 表示customerID的4字节整数
     * 表示customerName长度的4字节整数（如果customerName为空，则长度为0）
     * 表示customerName的N个字节
     */
    @Override
    public byte[] serialize(String topic, Customer data) {
        try {
            byte[] serializedName;
            int stringSize;

            if (Objects.isNull(data)) {
                return null;
            }

            if (Objects.nonNull(data.getName())) {
                serializedName = data.getName().getBytes(StandardCharsets.UTF_8);
                stringSize = serializedName.length;
            } else {
                serializedName = new byte[0];
                stringSize = 0;
            }

            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + stringSize);
            buffer.putInt(data.getID());
            buffer.putInt(stringSize);
            buffer.put(serializedName);

            return buffer.array();
        } catch (Exception e) {
            throw new SerializationException("Error when serializing Customer to byte[] " + e);
        }
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Customer data) {
        // 暂时不处理参数 headers
        return serialize(topic, data);
    }

    @Override
    public void close() {
        // 不需要关闭任何东西
    }
}

