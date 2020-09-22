package com.moqi.serializer;

import com.moqi.bean.Customer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * 自定义反序列化
 *
 * @author moqi
 * On 9/22/20 15:00
 */

public class ConsumerDeserializer implements Deserializer<Customer> {

    @Override
    public Customer deserialize(String topic, byte[] data) {
        int id, nameSize;
        String name;

        try {
            if (Objects.isNull(data)) {
                return null;
            }

            if (data.length < 8) {
                throw new SerializationException("Size of data received by IntegerDeserializer is shorter than expected");
            }

            ByteBuffer buffer = ByteBuffer.wrap(data);
            id = buffer.getInt();
            nameSize = buffer.getInt();

            byte[] nameBytes = new byte[nameSize];
            buffer.get(nameBytes);
            name = new String(nameBytes, StandardCharsets.UTF_8);

            return new Customer(id, name);
        } catch (Exception e) {
            throw new SerializationException("Error when serializing Customer to byte[]" + e);
        }
    }

}
