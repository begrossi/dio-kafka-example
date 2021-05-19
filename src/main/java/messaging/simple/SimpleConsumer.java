//Baseado em https://www.tutorialspoint.com/apache_kafka/apache_kafka_simple_producer_example.htm
package messaging.simple;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

public class SimpleConsumer {
    public static void main(String[] args) throws InterruptedException {
        if(args.length < 2){
            System.out.println("Enter topic name");
            return;
        }
        //Kafka consumer configuration settings
        String topicName = args[0];
        String group = args[1];

        //Configurações do Consumidor.
        //Veja mais em https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            //Kafka Consumer subscribes list of topics here.
            consumer.subscribe(Collections.singletonList(topicName));

            //print the topic name
            System.out.println("Inscrito para tópico " + topicName+ ", grupo "+group);


            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if(!records.isEmpty()) {
                    for (ConsumerRecord<String, String> record : records) {

                        // print the offset,key and value for the consumer records.
                        System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                    }
                }
            }
        }
    }
}