import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();

        kafkaProps.put("bootstrap.servers",  "localhost:9092");
        kafkaProps.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        kafkaProps.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        kafkaProps.put("schema.registry.url", "http://localhost:8081");
        kafkaProps.put("acks",  "all");

        KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<GenericRecord, GenericRecord>(kafkaProps);
        Schema valueSchema = SchemaBuilder.record("ValueRecord")
                .fields()
                .name("user").type("string").noDefault()
                .endRecord();

        try {
            for(int i = 0; i < 100; i++) {
                GenericRecord valueRecord = new GenericData.Record(valueSchema);
                valueRecord.put("user", "Admin Admin " + i);
                ProducerRecord<GenericRecord, GenericRecord> record =
                        new ProducerRecord<GenericRecord, GenericRecord>("avro-topic", null, valueRecord);
                producer.send(record);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
