package com.studies.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // Create Producer Configs
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=0; i < 10; i++) {
            // Create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("example_key_topic", "id_" + i, "Hello world: " + i);

            logger.info("key: " + record.key());

            // Send data
            producer.send(record, producerCallback()
            ).get(); // bad practice, this is synchronous (just to see the log)
        }

        // Flush
        producer.flush();

        // Flush and Close
        producer.close();
    }

    static Callback producerCallback(){
        return new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception error) {
                // Executes every time for success or error
                if(error == null){

                    logger.info("Receive metadata - " +
                            "\nTopic: " + recordMetadata.topic() +
                            "\nPartition: " + recordMetadata.partition() +
                            "\nOffSet: " + recordMetadata.offset() +
                            "\nTimestamp: " +  recordMetadata.timestamp());
                }else{
                    logger.error("Error while producer", error);
                }
            }
        };
    }
}
