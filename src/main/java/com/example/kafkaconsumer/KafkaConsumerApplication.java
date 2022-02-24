package com.example.kafkaconsumer;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaConsumerApplication {


    public static void main(String[] args) {
//        SpringApplication.run(KafkaConsumerApplication.class, args);
        final Logger logger = LoggerFactory.getLogger(KafkaConsumerApplication.class);
        Properties props = new Properties();


        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.144.11.73:9092,10.144.11.74:9092,10.144.11.75:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "NEC1_DEV_eos_tm_ordr_consumer");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");

//        "value.converter": "io.confluent.connect.avro.AvroConverter",
//            "value.converter.schema.registry.url": "http://10.144.11.79:8081,http://10.144.11.80:8081",
//            "key.converter": "io.confluent.connect.avro.AvroConverter",
//            "key.converter.schema.registry.url": "http://10.144.11.79:8081,http://10.144.11.80:8081"
//
//
        props.put("schema.registry.url", "http://10.144.11.79:8081,http://10.144.11.80:8081");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        String topic = "NEC1_DEV.eos.tm_ordr";
        final Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props);

        consumer.subscribe(Arrays.asList(topic));

        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(1000);
                for (ConsumerRecord<String, GenericRecord> record : records) {

                    logger.info("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());


                    System.out.println("haeseoky ordr_id = "+record.value().get("after"));

                }
            }
        } finally {
            consumer.close();
        }
    }

}
