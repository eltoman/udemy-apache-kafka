    package com.studies.kafka.tutorial1;

    import org.apache.kafka.clients.consumer.ConsumerConfig;
    import org.apache.kafka.clients.consumer.ConsumerRecord;
    import org.apache.kafka.clients.consumer.ConsumerRecords;
    import org.apache.kafka.clients.consumer.KafkaConsumer;
    import org.apache.kafka.common.errors.WakeupException;
    import org.apache.kafka.common.serialization.StringDeserializer;
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;

    import java.time.Duration;
    import java.util.Arrays;
    import java.util.Properties;
    import java.util.concurrent.CountDownLatch;

    public class ConsumerDemoWithThread {

        private static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        public static void main(String[] args) {
            new ConsumerDemoWithThread().run();
        }

        private ConsumerDemoWithThread(){
        }

        private void run(){
            CountDownLatch countDownLatch = new CountDownLatch(1);
            String bootStrapServer = "127.0.0.1:9092";
            String groupId = "my-sixth-application";
            String topic = "first_topic";

            logger.info("Creating the consumer");
            Runnable consumerRunnable = new ConsumerThread(countDownLatch, bootStrapServer, groupId, topic);

            Thread thread = new Thread(consumerRunnable);
            thread.start();

            // shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread( () -> {
                logger.info("Caught shutdown hook");
                ((ConsumerThread) consumerRunnable).shutdown();
                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    logger.info("Application has exited");
                }
            }));

            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                logger.error("Application is Interrupted", e);
            }finally {
                logger.info("Application is closing...");
            }
        }

    }

    class ConsumerThread implements Runnable{

        private CountDownLatch countDownLatch;
        KafkaConsumer<String, String> consumer;
        Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());

        public ConsumerThread(CountDownLatch countDownLatch,
        String bootStrapServer,
        String groupId,
        String topic) {

            this.countDownLatch = countDownLatch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try{
                while (true){
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        logger.info("Key: " +  record.key() + " value: " + record.value());
                        logger.info("Partition: " +  record.partition() +  " Offset: " + record.offset());
                    }
                }
            }catch (WakeupException ex){
                logger.info("Received Shutdown signal! ", ex.getMessage());
            }
            finally {
                consumer.close();
                countDownLatch.countDown();
            }
        }

        public void shutdown(){
            // special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
