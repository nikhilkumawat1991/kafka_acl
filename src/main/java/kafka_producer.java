import org.apache.kafka.clients.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.*;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Properties;

public class kafka_producer {

    private final static String TOPIC = "my-example-topic";
    private final static String BOOTSTRAP_SERVERS = "192.168.1.103:9092";
    private final static String GROUP_NAME = "FirstConsumerGroup";


    private  static KafkaProducer<String, String> createProducer()
    {
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG,"Kafka-Acl-Producer");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.1.103:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        return new KafkaProducer<String, String>(props);

    }


    private  static KafkaConsumer<String, String> createConsumer()
    {
        Properties props_consumer = new Properties();
        props_consumer.put(ConsumerConfig.GROUP_ID_CONFIG,GROUP_NAME);
        props_consumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.1.103:9092");
        props_consumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props_consumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        return new KafkaConsumer<>(props_consumer);

    }

    private static void runProducer()
    {
        KafkaProducer sample_producer = createProducer();
        try
        {
            for (int i =0; i<10; i++)
            {
                ProducerRecord<String,String> record = new ProducerRecord<>(TOPIC,"Nikhil"+i);

                //Synchronous Producer send method. Will wait for the the reply from the broker and then send next message

                //RecordMetadata metadata = (RecordMetadata) sample_producer.send(record).get();
                //System.out.println("message is sent to: "+metadata.partition()+" and offset is: "+metadata.offset());
                //System.out.println("Message sent successfully");


                //Asynchronous Producer send method. Will not wait for the reply

                sample_producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null)
                        {
                            e.printStackTrace();
                            System.out.println("error occured while sending record ");
                        }
                        else
                        {
                            System.out.println("Message sent successfully..!!");
                        }
                    }
                });

            }

        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.out.println("Error occured while sending the message..!!");
        }
        finally
        {
            sample_producer.close();
        }

    }


    private static void runConsumer()
    {
        KafkaConsumer<String, String> sample_consumer = createConsumer();
        sample_consumer.subscribe(Arrays.asList(TOPIC));
        System.out.println("Subscribed to topic for consuming data.");

        while (true)
        {
            ConsumerRecords<String,String> record_consumer = sample_consumer.poll(10);

            for(ConsumerRecord<String, String> record: record_consumer)
            {
                System.out.println("inside loop");
                System.out.println("ID is: "+ String.valueOf(record.value().getBytes(StandardCharsets.UTF_8))+ "and value is: "+record.value().getBytes(StandardCharsets.UTF_8));
            }
        }
    }


    public static void main(String[] args)
    {

        //runProducer();
        runConsumer();

    }
}
