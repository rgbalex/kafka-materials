package clients.airport.consumers.totals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.Arrays;
import java.util.List;

// Needs tidying up
import java.time.*;
import java.time.format.*;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.ValueJoiner;

import clients.airport.AirportProducer;
import clients.airport.AirportProducer.TerminalInfo;

/**
 * Simple consumer which just counts totals over checkins. This is very simplistic: it doesn't
 * handle rebalancing, and wouldn't scale as it doesn't apply any windows or splits the input
 * in any particular way.
 */
// public class TotalCheckinsConsumer extends AbstractInteractiveShutdownConsumer {
public class TotalCheckinsConsumer {

	// public void run() {
		// Properties props = new Properties();
		// props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
		// props.put("group.id", "total-checkins");
		// props.put("enable.auto.commit", "true");
		
		// int started = 0, completed = 0, cancelled = 0;

		// try (KafkaConsumer<Integer, TerminalInfo> consumer = new KafkaConsumer<>(props, new IntegerDeserializer(), new TerminalInfoDeserializer())) {
		// 	consumer.subscribe(Arrays.asList(AirportProducer.TOPIC_CHECKIN, AirportProducer.TOPIC_COMPLETED, AirportProducer.TOPIC_CANCELLED));

		// 	while (!done) {
		// 		ConsumerRecords<Integer, TerminalInfo> records = consumer.poll(Duration.ofSeconds(1));
		// 		if (records.isEmpty()) continue;

		// 		Instant latestInstant = null;
		// 		for (ConsumerRecord<Integer, TerminalInfo> record : records) {
		// 			Instant recordTime = Instant.ofEpochMilli(record.timestamp());
		// 			if (latestInstant == null || latestInstant.isBefore(recordTime)) {
		// 				latestInstant = recordTime;
		// 			}

		// 			switch (record.topic()) {
		// 			case AirportProducer.TOPIC_CHECKIN:
		// 				++started; break;
		// 			case AirportProducer.TOPIC_COMPLETED:
		// 				++completed; break;
		// 			case AirportProducer.TOPIC_CANCELLED:
		// 				++cancelled; break;
		// 			}
		// 		}

		// 		System.out.printf("Checkins at %s: %d started, %d completed, %d cancelled%n", latestInstant, started, completed, cancelled);
		// 	}
		// }


	/**
	 * Runs the consumer until the user presses Enter. This works around a limitation
	 * in Eclipse that Ctrl+C cannot be sent from the Console view. Gradle's JavaExec
	 * task has a similar limitation.
	 */
	// public static void main(String[] args) {
	// 	new StatusPrinterConsumer().runUntilEnterIsPressed(System.in);
	// }

	public KafkaStreams run() 
	{
		// 1. Use StreamsBuilder to build the topology
		StreamsBuilder builder = new StreamsBuilder();
		Serde<TerminalInfo> terminalInfoSerde = new AirportProducer.TerminalInfoSerde();

		// Consume the status updates from the TOPIC_CHECKIN (Integer key and TerminalInfo value)
		final KStream<Integer, TerminalInfo> checkinKStream = builder.stream(AirportProducer.TOPIC_CHECKIN, Consumed.with(Serdes.Integer(), terminalInfoSerde));
		final KStream<Integer, TerminalInfo> completedKStream = builder.stream(AirportProducer.TOPIC_COMPLETED, Consumed.with(Serdes.Integer(), terminalInfoSerde));
		final KStream<Integer, TerminalInfo> cancelledKStream = builder.stream(AirportProducer.TOPIC_CANCELLED, Consumed.with(Serdes.Integer(), terminalInfoSerde));
		
		
		
		final KTable<String, Long> checkinTimeStampKTable = 
		checkinKStream
		.processValues(() -> new TimestampProcessor<Integer, TerminalInfo>())
		.selectKey((k, v) -> Instant.ofEpochMilli(v).atZone(ZoneOffset.UTC).format(DateTimeFormatter.BASIC_ISO_DATE))
		.groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
		.count();
		
		final KTable<String, Long> completedTimeStampKTable = 
		completedKStream
		.processValues(() -> new TimestampProcessor<Integer, TerminalInfo>())
		.selectKey((k, v) -> Instant.ofEpochMilli(v).atZone(ZoneOffset.UTC).format(DateTimeFormatter.BASIC_ISO_DATE))
		.groupByKey()
		.count();
		
		final KTable<String, Long> cancelledTimeStampKTable = 
		cancelledKStream
		.processValues(() -> new TimestampProcessor<Integer, TerminalInfo>())
		.selectKey((k, v) -> Instant.ofEpochMilli(v).atZone(ZoneOffset.UTC).format(DateTimeFormatter.BASIC_ISO_DATE))
		.groupByKey()
		.count();
		
		// // Print out checkin information for the day
		// final KStream<String, Long> checkinsByDayKStream = checkinTimeStampKTable.toStream();
		// checkinsByDayKStream.mapValues((k, v) -> String.format("Status from %s: %d total checkins%n", k, v))
		// .print(Printed.toSysOut());
		
		ValueJoiner<Long, Long, List<Long>> longValueJoiner = (firstLong, secondLong) -> {
			List<Long> foobar = Arrays.asList(firstLong, secondLong);
			return foobar;
		};
		
		ValueJoiner<List<Long>, Long, List<Long>> longlistValueJoiner = (firstLong, secondLong) -> {
			firstLong.add(secondLong);
			return firstLong;
		};

		// ValueJoiner<Long, Long, String> longArrayValueJoiner = (firstLong, secondLong) -> {
		// 	return String.format("Total Checkins:%d, Completed: %d", firstLong, secondLong);
		// };

		// ValueJoiner<Long, String, String> longStringValueJoiner = (firstLong, firstString) -> {
		// 	return String.format("%s, Cancelled: %d", firstString, firstLong);
		// };

		// final String TOPIC_CHECKINS_BY_DAY = "selfservice-checkins-by-day";

		// checkinTimeStampKTable.toStream().to(TOPIC_CHECKINS_BY_DAY);
		
		final KStream<String, List<Long>> totalKStream = 
			checkinTimeStampKTable.join(
										completedTimeStampKTable,
										longValueJoiner)
									.join(
										cancelledTimeStampKTable, 
										longlistValueJoiner).toStream();

		totalKStream.mapValues((l1) -> String.format("Date: %s; Total Checkins %d, Completed Checkins %d, Cancelled Checkins %d", 
				l1.get(0), l1.get(1), l1.get(2)))
			.print(Printed.toSysOut());

		// totalKStream = totalKStream.join(cancelledTimeStampKTable,
		// 								valueJoiner,
		// 								JoinWindows.of(Duration.ofSeconds(10)));
		
		// 2. Configure the Properties (need bootstrap serves and app ID at least)
		Properties props = new Properties();
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AirportProducer.BOOTSTRAP_SERVERS);
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-status");
		props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
		
		// 3. Use the builder to create the low-level Topology, set up shutdown hook, and start the app
		KafkaStreams kStreams = new KafkaStreams(builder.build(), props);
		Runtime.getRuntime().addShutdownHook(new Thread(kStreams::close));
		kStreams.start();
		
		return kStreams;
	}

	public static void main(String[] args) {
		KafkaStreams kStreams = new TotalCheckinsConsumer().run();

		// Shut down the application after pressing Enter in the Console
		try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
			br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			kStreams.close();
		}
	}

}
