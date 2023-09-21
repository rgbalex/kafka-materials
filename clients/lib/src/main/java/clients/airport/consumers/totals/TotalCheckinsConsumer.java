package clients.airport.consumers.totals;

import java.util.Properties;

import clients.airport.consumers.AbstractInteractiveShutdownConsumer;
import clients.messages.MessageProducer;

/**
 * Consumer that reports total numbers of started, completed, and cancelled
 * checkins. The first version is very simplistic and won't handle rebalancing.
 * This overall computation wouldn't scale well anyway, as it doesn't apply any
 * windows or split the input in any particular way.
 */
public class TotalCheckinsConsumer extends AbstractInteractiveShutdownConsumer {

	public void run() {
		Properties props = new Properties();
		props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
		props.put("group.id", "total-checkins");
		props.put("enable.auto.commit", "true");
		
		int started = 0, completed = 0, cancelled = 0;

		// TODO: exercise

	}

	public static void main(String[] args) {
		new TotalCheckinsConsumer().runUntilEnterIsPressed(System.in);
	}

}
