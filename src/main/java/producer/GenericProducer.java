package producer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class GenericProducer {

    public static void main(String[] args) throws IOException {
        GenericProducer genericRecordProducer = new GenericProducer();
        genericRecordProducer.writeMessage();
    }

    public void writeMessage() throws IOException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.140.0.3:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://10.140.0.3:8081");

        Producer<String, GenericRecord> producer = new KafkaProducer<>(properties);

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(new File("/home/quanghuy/KafkaProject/huylq78.avro/src/main/avro/userInfo.avsc"));

        //prepare the avro record
        GenericRecord info = new GenericData.Record(schema);
        info.put("username", "Hello world");
        info.put("age", 20);
        info.put("phone","0");
        GenericRecord address = new GenericData.Record(schema.getField("address").schema());
        address.put("street", "a");
        address.put("city", "b");
        address.put("country","c");
        info.put("address", address);
        System.out.println(info);

        //prepare the kafka record
        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("avro-topic", null, info);

        producer.send(record);
        //ensures record is sent before closing the producer
        producer.flush();

        producer.close();
    }
}
