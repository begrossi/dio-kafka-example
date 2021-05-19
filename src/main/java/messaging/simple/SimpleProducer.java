//Baseado em https://www.tutorialspoint.com/apache_kafka/apache_kafka_simple_producer_example.htm
package messaging.simple;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class SimpleProducer {
    public static void main(String[] args) throws InterruptedException {
        if(args.length == 0){
            System.out.println("Informe o nome do tópico como primeiro argumento");
            return;
        }

        //Guarda o nome do tópico
        String topicName = args[0];

        //Configurações do Produtor.
        //Veja mais em https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html
        Properties props = new Properties();
        //Define localhost como servidor kafka
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //Define a espera por todos os acks, garantindo que não será perdido.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //Evitar retentativa em caso de falha.
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        //Especifica o tamanho padrão do buffer.
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //Reduz o número de requisições, esperando outras até o limite de tempo e do batch.size.
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //Quantidade de memória disponível para buffering.
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        //Define serializadores (conversores) para chave e valor. O dado sempre trafega como byte[].
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {

            for (int i=0; i<100; i++) {
                System.out.println("Enviando mensagem "+i+" para tópico "+topicName);
                producer.send(new ProducerRecord<>(topicName, "Evento "+i, "Valor "+i));

                Thread.sleep(1000);
            }

            System.out.println("Eventos enviados com sucesso");

        }
    }
}
