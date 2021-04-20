import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;

import java.util.Date;
import java.util.Properties;

import static org.slf4j.LoggerFactory.getLogger;

public class SimpleProducerWithCallback {
    public static void main(String[] args) {

        Logger logger = getLogger(SimpleProducerWithCallback.class);
        // Generate total consecutive events starting with ufoId
        long total = Long.parseLong("10");
        long ufoId = Math.round(Math.random() * Integer.MAX_VALUE);

        // Set up client Java properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "172.25.38.193:6667");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
//send data
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (long i = 0; i < total; i++) {
                String key = Long.toString(ufoId++);
                long runtime = new Date().getTime();
                double latitude = (Math.random() * (2 * 85.05112878)) - 85.05112878;
                double longitude = (Math.random() * 360.0) - 180.0;
                String msg = runtime + "," + latitude + "," + longitude;
                //create a producer record
                try {
                    ProducerRecord<String, String> data = new
                            ProducerRecord<String, String>("ufo_sightings", key, msg);
                    //Send data asynchronous
                    producer.send(data, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            //executes everytime a record is successfully sent or an exception is thrown

                            if ( e==null)
                            //data was successfully sent
                                {
                                    logger.info("Received new metadata. \n" + "Topic:" + recordMetadata.topic());
                                } else {
                                logger.error ( "Error while producing" , e);
                            }
                        }
                    });
                    long wait = Math.round(Math.random() * 25);
                    Thread.sleep(wait);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
