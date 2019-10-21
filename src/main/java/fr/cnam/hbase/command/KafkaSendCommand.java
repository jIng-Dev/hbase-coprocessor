package fr.cnam.hbase.command;

import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import fr.cnam.hbase.command.KafkaSendCommand;

import com.google.common.collect.ImmutableMap;

public class KafkaSendCommand {

	private static KafkaSendCommand instance = new KafkaSendCommand();
	private KafkaProducer<String, String> producer;
	private String bootstrap;

	private KafkaSendCommand() {
	}

	public void initBootstrap(String bootstrap) {
		this.bootstrap = bootstrap;
		this.producer = new KafkaProducer<>(
				ImmutableMap.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrap,
						ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()),
				new StringSerializer(), new StringSerializer());
	}

	public String sendRecord(String data) {
		String topicName = "messages";
		this.producer.send(new ProducerRecord<>(topicName, "testcontainers", data));
		System.err.println("Producer send : " + data);
		return data;
	}

	public static final KafkaSendCommand getInstance() {
		return instance;
	}
}
