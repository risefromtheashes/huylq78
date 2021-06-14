package week2.producer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

public class NoSchemaRegistryProducer {
    public static void main(String[] str) throws InterruptedException, IOException {
        System.out.println("Starting ProducerAvroExample ...");
        sendMessages();
    }
    private static void sendMessages() throws InterruptedException, IOException {
        Producer<String, byte[]> producer = createProducer();
        sendRecords(producer);
    }
    private static Producer<String, byte[]> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.140.0.3:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
        return new KafkaProducer(props);
    }
    private static void sendRecords(Producer<String, byte[]> producer) throws IOException, InterruptedException {
        String topic = "avro-topic";
        int partition = 0;

            producer.send(new ProducerRecord<String, byte[]>(topic, 0, Integer.toString(1), record()));
            Thread.sleep(500);

    }
    private static byte[] record() throws IOException {
        GenericRecord info = new GenericData.Record(AvroSupport.getSchema());
        info.put("username", "huy");
        info.put("age", 23);
        info.put("phone", "01");
        GenericRecord mailing_address = new GenericData.Record(AvroSupport.getSchema().getField("address").schema());
        mailing_address.put("street", "ph");
        mailing_address.put("city", "hn");
        mailing_address.put("country", "vn");
        info.put("address", mailing_address);
        return AvroSupport.dataToByteArray(AvroSupport.getSchema(), info);
    }
}
