import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers",  "localhost:9092");
        kafkaProps.put("group.id", "new-group");
        kafkaProps.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        kafkaProps.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        kafkaProps.put("schema.registry.url", "http://localhost:8081");

        KafkaConsumer consumer = new KafkaConsumer<GenericRecord, GenericRecord>(kafkaProps);
        consumer.subscribe(Collections.singletonList("avro-topic"));
        while (true) {
            ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(1000);
            for (ConsumerRecord<GenericRecord, GenericRecord> record: records) {
                System.out.println("Value: " + record.value().get("user"));
            }
            consumer.commitSync();
        }
    }
}