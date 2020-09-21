package com.moqi.serializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * AvroDemo
 *
 * schema.registry.url 的问题，参考 [How to set schema.registry.URL?](https://stackoverflow.com/a/51904064),
 * 先从这里 [Confluent Platform](https://www.confluent.io/download/) 下载一个 Self managed event streaming platform,
 * 填写 email，选择手动部署，选择 tar，最终下载一个类似名为 confluent-5.5.1-2.12.tar.gz 的文件，然后解压，
 * 进入文件夹 etc/schema-registry/schema-registry.properties 设定 listeners=http://localhost:8081,
 * 然后返回文件夹顶级目录运行 bin/schema-registry-start etc/schema-registry/schema-registry.properties 即可。
 *
 * @author moqi
 * On 9/21/20 17:05
 */

public class AvroDemo {

    private static final Random random = new Random();

    public static void main(String[] args) {
        Producer<String, GenericRecord> producer = getProducer();

        Schema schema = getSchema();

        for (int i = 0; i < 100; i++) {
            int randomCode = random.nextInt(100000);
            String name = "ExampleCustomer" + randomCode;
            String email = "Example" + randomCode + "@example.com";

            GenericRecord customer = new GenericData.Record(schema);
            customer.put("id", randomCode);
            customer.put("name", name);
            customer.put("email", email);

            ProducerRecord<String, GenericRecord> data = new ProducerRecord<>("test", name, customer);
            producer.send(data);
        }
    }

    private static Producer<String, GenericRecord> getProducer() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        kafkaProps.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        kafkaProps.put("schema.registry.url", "http://localhost:8081");

        return new KafkaProducer<>(kafkaProps);
    }

    private static Schema getSchema() {
        //language=JSON
        String schemaString = "{\n" +
                "  \"namespace\": \"customerManagement.avro\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"Customer\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"name\": \"id\",\n" +
                "      \"type\": \"int\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"name\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"email\",\n" +
                "      \"type\": \"string\",\n" +
                "      \"default\": \"null\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }

}
