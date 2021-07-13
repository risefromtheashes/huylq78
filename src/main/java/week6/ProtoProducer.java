package week6;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import week6.datatracking.Datatracking;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import week6.datatracking.Datatracking;

import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;

public class ProtoProducer {
    public static void main(String[] args) {
        Faker faker = new Faker(Locale.forLanguageTag("vi"));

        Random generator = new Random(5);
        LocalTime time = LocalTime.MIN.plusSeconds(generator.nextLong());

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "10.140.0.3:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class);

        KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(properties);
        ProducerRecord<String, byte[]> producerRecord = null;

        long offset = Timestamp.valueOf("2019-01-01 00:00:00").getTime();
        long end = Timestamp.valueOf("2022-01-01 00:00:00").getTime();
        long diff = end - offset + 1;

        for(int i = 0 ; i < 10 ; i++){
            Timestamp rand = new Timestamp(offset + (long)(Math.random() * diff));
            Datatracking.DataTracking message = Datatracking.DataTracking.newBuilder()
                    .setVersion(String.valueOf(faker.number().numberBetween(1,100)))
                    .setName(faker.name().fullName())
                    .setTimestamp(rand.getTime())
                    .setPhoneId(faker.phoneNumber().phoneNumber())
                    .setLon(faker.number().numberBetween(1,100))
                    .setLat(faker.number().numberBetween(1,100))
                    .build();
            producerRecord = new ProducerRecord<String, byte[]>("testproto1", null, message.toByteArray());
            producer.send(producerRecord);
        }
//


        producer.flush();
        producer.close();
    }
}

