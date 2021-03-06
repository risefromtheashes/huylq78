package week2.consumer;
import week2.producer.AvroSupport;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;


/**
 * Reads an avro message.
 */
public class NoSchemaRegistryConsumer {

    public static void main(String[] str) throws InterruptedException, IOException {

        System.out.println("Starting AutoOffsetAvroConsumerExample ...");

        readMessages();


    }

    private static void readMessages() throws InterruptedException, IOException {

        KafkaConsumer<String, byte[]> consumer = createConsumer();

        // Assign to specific topic and partition, subscribe could be used here to subscribe to all topic.
        consumer.assign(Arrays.asList(new TopicPartition("avro-topic", 0)));

        processRecords(consumer);
    }

    private static void processRecords(KafkaConsumer<String, byte[]> consumer) throws InterruptedException, IOException {

        while (true) {

            ConsumerRecords<String, byte[]> records = consumer.poll(100);
            for (ConsumerRecord<String, byte[]> record : records) {
                GenericRecord genericRecord = AvroSupport.byteArrayToData(AvroSupport.getSchema(), record.value());

                System.out.println(genericRecord.get("address"));
            }

            consumer.commitSync();
            Thread.sleep(500);

        }
    }

    private static KafkaConsumer<String, byte[]> createConsumer() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.140.0.3:9092");
        String consumeGroup = "cg1";
        props.put("group.id", consumeGroup);
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        return new KafkaConsumer<String, byte[]>(props);
    }


}
