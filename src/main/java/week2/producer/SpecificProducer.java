package week2.producer;
import week2.*;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import week2.mailing_address;
import week2.userInfo;

import java.util.Properties;

public class SpecificProducer {
    public static void main(String[] args) {
        SpecificProducer producer = new SpecificProducer();
        producer.sendMessage();
    }
    public void sendMessage(){
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.140.0.3:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,   KafkaAvroSerializer.class);
        prop.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://10.140.0.3:8081");
        Producer<String, SpecificRecord> producer = new KafkaProducer<String, SpecificRecord>(prop);
        userInfo info = new userInfo();
        info.setUsername("huy");
        info.setAge(23);
        info.setPhone("012");
        info.getAddress();
        mailing_address address = new mailing_address();
        address.setCountry("vn");
        address.setCity("hn");
        address.setStreet("ph");
        info.setAddress(address);
        ProducerRecord<String, SpecificRecord> record = new ProducerRecord<>("avro-topic", info);
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
