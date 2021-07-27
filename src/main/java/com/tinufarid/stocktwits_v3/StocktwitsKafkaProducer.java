package com.tinufarid.stocktwits_v3;

import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class StocktwitsKafkaProducer {

    Logger logger = LoggerFactory.getLogger(StocktwitsKafkaProducer.class.getName());

    public static void main(String[] args) throws UnirestException, InterruptedException {
        new StocktwitsKafkaProducer().run();
    }

    public void run() throws UnirestException, InterruptedException {

        logger.info("Setup");

        // create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        while (true) {
            String msg = null;
            JSONArray messages = StockTwistsAPIProducer.ApiProducer();


            String messageKey = "";
            String messageValue = "";
            String messageCreatedAt = "";
            String messageSentiment = "";


            for (int i = 0; i < messages.length(); i++) {

                JSONObject message = messages.getJSONObject(i);

                messageKey = message.getBigInteger("id").toString();
                messageValue = message.getString("body");
                messageCreatedAt = message.getString("created_at");
                messageSentiment = message.getJSONObject("entities").toString();

                JSONObject kafkaMessage = new JSONObject();
                kafkaMessage.put("id", messageKey);
                kafkaMessage.put("body", messageValue);
                kafkaMessage.put("created_at", messageCreatedAt);
                kafkaMessage.put("sentiment", messageSentiment);


                        producer.send(new ProducerRecord<>(ApplicationConfig.topic, messageKey, kafkaMessage.toString()), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e != null) {
                                logger.error("Something bad happened", e);
                            }
                        }
                    });


            }
            TimeUnit.SECONDS.sleep(60);
        }
    }


    public KafkaProducer<String, String> createKafkaProducer(){
        String bootstrapServers = ApplicationConfig.bootstrapServers;

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // kafka 2.0 >= 1.1 so we can keep this as 5. Use 1 otherwise.

        // high throughput producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 KB batch size

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}
