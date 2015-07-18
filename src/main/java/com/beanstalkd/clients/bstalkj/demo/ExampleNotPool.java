package com.beanstalkd.clients.bstalkj.demo;

import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beanstalkd.clients.bstalkj.BeanstalkFactory;
import com.beanstalkd.clients.bstalkj.BeanstalkJob;
import com.beanstalkd.clients.bstalkj.Client;
import com.beanstalkd.clients.bstalkj.Producer;
import com.beanstalkd.clients.bstalkj.Worker;

public class ExampleNotPool {

	protected static Logger log = LoggerFactory.getLogger(ExampleNotPool.class);

	public static void main(String[] args) {
		notPooledExample();
	}

	public static void notPooledExample() {
		BeanstalkFactory factory = BeanstalkFactory.factory().build();

		Client client = factory.get();

		double producerTime = timedMethod(ExampleNotPool::notPooledParallelProducer, client);
		System.out.println("Producer time taken: " + producerTime);

		double consumerTime = timedMethod(ExampleNotPool::notPooledParallelConsumer, client);
		System.out.println("Consumer time taken: " + consumerTime);

		System.out.println("Total time taken: " + (consumerTime + producerTime));
	}

	private static void notPooledParallelProducer(final Client client) {
		Producer producer = client.producer();
		IntStream.range(0, 10).boxed().forEach(f -> {
			log.info("Job going to be created: {}", f);
			producer.put(1l, 0, 5000, "this is some data".getBytes());
			log.info("Job created: {}", f);
		});
		log.info("All Jobs enqueue");
		log.info(producer.tubeStats());
		log.info(producer.toString());
	}

	private static void notPooledParallelConsumer(final Client client) {
		Worker worker = client.worker();
		IntStream.range(0, 10).boxed().forEach(f -> {
			BeanstalkJob job = worker.reserve(60).get();
			log.info("Got job: " + job);
			worker.delete(job);
		});
		log.info(worker.tubeStats());
		log.info(worker.toString());
	}

	private static double timedMethod(Consumer<Client> function, Client client) {
		long time = System.nanoTime();
		function.accept(client);
		return (System.nanoTime() - time) / 1_000_000.0;
	}
}
