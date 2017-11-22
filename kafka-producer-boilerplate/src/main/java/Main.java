import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();

        kafkaProps.put("bootstrap.servers",  "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("acks",  "all");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);

        try {
            for(int i = 0; i < 100; i++) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<String, String>("string-topic", "key", "value-" + i);
                producer.send(record);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }
}
