package consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import nhom1.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SpecificConsumer {
    public static void main(String[] args) {
        SpecificConsumer consumer = new SpecificConsumer();
        consumer.readMessage();
    }
    public void readMessage(){
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.140.0.3:9092");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "specific-record-consumer-group2");
        prop.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "10.140.0.3:8081");
        prop.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        Consumer<String, userInfo> consumer = new KafkaConsumer<String, userInfo>(prop);
        consumer.subscribe(Collections.singleton("avro-topic"));
        while (true) {
            ConsumerRecords<String, userInfo> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, userInfo> record : records) {
                System.out.println("name: " + record.value().getUsername());
                System.out.println("age: " + record.value().getAge());
                System.out.println("Address: " + record.value().getAddress().getCountry());
                System.out.println(record.value());
            }
            consumer.commitAsync();
        }
    }
}
