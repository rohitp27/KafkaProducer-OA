package com.pandey.rohit.kafkaproducer;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class DemoProducer {
	public static void main(String[] args) throws InterruptedException {
		System.out.println("Data Producer for Apache Kafka");
		
		long messageCounter = 0;
		
		Properties props = new Properties();
		props.put("zookeeper.connect", "zookeeper1:2181, zookeeper2:2181");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("bootstrap.servers", "kafka1:9092, kafka2:9092");
		
		Producer producer = new KafkaProducer<String, String>(props);
		ProducerRecord<String, String> producerRecord = null;
		while(true) {
			Thread.sleep(100);
			producerRecord = new ProducerRecord<String, String>("onlineDataStream", "", UUID.randomUUID().toString());
			producer.send(producerRecord);
			messageCounter++;
			if (messageCounter %100 == 0) {
				System.out.println("messageCounter = " + messageCounter);
			}
		}
	}
}