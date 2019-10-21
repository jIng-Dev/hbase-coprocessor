package fr.cnam.hbaseCoprocessor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import fr.cnam.hbase.command.KafkaSendCommand;
import fr.cnam.hbaseCoprocessor.HbaseMiniCluster;
import org.junit.Rule;
import org.junit.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.KafkaContainer;

import com.google.common.collect.ImmutableMap;

public class HbaseToKafkaCoprocessor {

	@Rule
	public KafkaContainer cKafka = new KafkaContainer("latest");
	
	@Test
	public void test() {
		KafkaSendCommand.getInstance().initBootstrap(cKafka.getBootstrapServers());
		HbaseMiniCluster miniCluster = new HbaseMiniCluster();
		miniCluster.start();
		miniCluster.initTable();
		miniCluster.ingestTable();
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
				ImmutableMap.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cKafka.getBootstrapServers(),
						ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
						ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
				new StringDeserializer(), new StringDeserializer());
		String topicName = "messages";
		consumer.subscribe(Arrays.asList(topicName));
		Unreliables.retryUntilTrue(1000, TimeUnit.SECONDS, () -> {
			ConsumerRecords<String, String> records = consumer.poll(100);
			if (records.isEmpty()) {
				return false;
			}
			System.err.println("Records size : " + records.count());
//			records.iterator()
			assertThat(records).hasSize(25)
					.extracting(ConsumerRecord::topic, ConsumerRecord::key, ConsumerRecord::value)
					.contains(tuple(topicName, "testcontainers", "value129"));
//
			return true;
		});
		consumer.unsubscribe();
		miniCluster.stopCluster();
	}

}
