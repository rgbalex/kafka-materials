package clients.airport.consumers.totals;

import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

public class TimestampProcessor<K, V> extends ContextualFixedKeyProcessor<K, V, Long> {
    @Override
    public void process(FixedKeyRecord<K, V> record) {
        context().forward(record.withValue(record.timestamp()));
    }
}