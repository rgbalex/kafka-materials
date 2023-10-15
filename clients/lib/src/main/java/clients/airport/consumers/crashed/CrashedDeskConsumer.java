package clients.airport.consumers.crashed;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import clients.airport.AirportProducer;
import clients.airport.AirportProducer.TerminalInfo;
import clients.airport.AirportProducer.TerminalInfoDeserializer;
import clients.airport.consumers.AbstractInteractiveShutdownConsumer;
import clients.messages.MessageProducer;

/**
 * Naive version of a 'possibly down' consumer, which now does consider rebalancing.
 */
public class CrashedDeskConsumer extends AbstractInteractiveShutdownConsumer {

	Map<Integer, Map<Integer, Instant>> lastHeartbeat = new TreeMap<>();

	public void run() {
		Properties props = new Properties();
		props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
		props.put("group.id", "crashed-desks-simple");

		// Kafka will auto-commit every 5s based on the last poll() call
		props.put("enable.auto.commit", "true");

		Map<Integer, TopicPartition> assignedPartitions = new HashMap<Integer, TopicPartition>();

		try (KafkaConsumer<Integer, TerminalInfo> consumer = new KafkaConsumer<>(props, new IntegerDeserializer(), new TerminalInfoDeserializer())) {
			consumer.subscribe(Collections.singletonList(AirportProducer.TOPIC_STATUS), new ConsumerRebalanceListener() {
			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				System.out.printf("onPartitionsRevoked - partitions: %s%n", formatPartitions(partitions));
				
				// if we decide to clear up on partition removed, do the following:
				boolean clean_up = true;

				for (TopicPartition partition: partitions)
				{
					if (assignedPartitions.containsKey(partition.partition()))
						assignedPartitions.remove(partition.partition());

					if (!clean_up)
						return;

					if (lastHeartbeat.containsKey(partition.partition()))
						lastHeartbeat.remove(partition.partition());
				}
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				System.out.printf("onPartitionsAssigned - partitions: %s%n", formatPartitions(partitions));
				for (TopicPartition partition: partitions)
				{
					if (!assignedPartitions.containsKey(partition.partition()))
					assignedPartitions.put(partition.partition(), partition);
					
					
					if (!lastHeartbeat.containsKey(partition.partition()))
					lastHeartbeat.put(partition.partition(), new TreeMap<Integer, Instant>());
				}
				
				// reset all known partitions to the beginning (warning: costly)
				consumer.seekToBeginning(partitions);
			}
			});

			while (!done) {
				ConsumerRecords<Integer, TerminalInfo> records = consumer.poll(Duration.ofSeconds(5));
				for (ConsumerRecord<Integer, TerminalInfo> record : records) {
					if (!assignedPartitions.containsKey(record.partition()))
						// This consumer must not be listening to this partition
						continue;

					// Otherwise, log the message 
					// Desk ID -> Status
					Map<Integer, Instant> heartbeat_partition = lastHeartbeat.get(record.partition());
					
					heartbeat_partition.put(record.key(), Instant.ofEpochMilli(record.timestamp()));
				}

				// If we sped up the simulation 10x, we should be getting status events every 6 seconds
				// We double that so we are sure we definitely missed a status update.
				final Instant oneMinuteAgo = Instant.now().minusSeconds(12);

				int i = 1;
				System.out.printf("Possibly down terminals as of %s:%n", Instant.now());
				for (Entry<Integer, Map<Integer, Instant>> partition : lastHeartbeat.entrySet()) {		
					for (Map.Entry<Integer, Instant> entry : partition.getValue().entrySet())
					{
						if (entry.getValue().isBefore(oneMinuteAgo)) {
							System.out.printf("%3d. %d (no status heartbeat since %s) on partition %d %n", i++, entry.getKey(), entry.getValue(), partition.getKey());
						}
					}
				}
				System.out.println();
			}
		}
	}

	private static List<String> formatPartitions(Collection<TopicPartition> partitions) {
	return partitions.stream().map(topicPartition ->
			String.format("topic: %s, partition: %s", topicPartition.topic(), topicPartition.partition()))
						.collect(Collectors.toList());
	}

	public static void main(String[] args) {
		new CrashedDeskConsumer().runUntilEnterIsPressed(System.in);
	}
}
