package clients.airport.consumers.totals;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

import clients.airport.AirportProducer;
import clients.messages.MessageProducer;

public class StreamsTotalCheckinsByDayConsumer {

	public class TimestampProcessor<K, V> extends ContextualFixedKeyProcessor<K, V, Long> {
		@Override
		public void process(FixedKeyRecord<K, V> record) {
			context().forward(record.withValue(record.timestamp()));
		}
	}

	private static final String TOPIC_CHECKINS_BY_DAY = "selfservice-checkins-by-day";

	public void run() throws IOException {
		StreamsBuilder builder = new StreamsBuilder();

		// NOTE: this is using local date, rather than event date - would need a processor to replace value
		final AirportProducer.TerminalInfoSerde tInfoSerde = new AirportProducer.TerminalInfoSerde();
		KStream<Integer, Long> timestampStream = builder
			.stream(AirportProducer.TOPIC_CHECKIN, Consumed.with(Serdes.Integer(), tInfoSerde))
			.processValues(TimestampProcessor::new);

		KGroupedStream<String, Long> dayGroupedStream = timestampStream
			.selectKey((k, v) -> Instant.ofEpochMilli(v).atZone(ZoneOffset.UTC).format(DateTimeFormatter.BASIC_ISO_DATE))
			.groupByKey(Grouped.with(Serdes.String(), Serdes.Long()));

		KTable<String, Long> dayCountsTable = dayGroupedStream.count();

		dayCountsTable.toStream().to(TOPIC_CHECKINS_BY_DAY);

		Properties props = new Properties();
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, MessageProducer.BOOTSTRAP_SERVERS);
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "total-checkins");

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		streams.start();
	}

	public static void main(String[] args) {
		try {
			new StreamsTotalCheckinsByDayConsumer().run();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
