package com.beanstalkd.clients.bstalkj.demo;

import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beanstalkd.clients.bstalkj.BeanstalkFactory;
import com.beanstalkd.clients.bstalkj.BeanstalkJob;
import com.beanstalkd.clients.bstalkj.Producer;
import com.beanstalkd.clients.bstalkj.Worker;
import com.beanstalkd.clients.bstalkj.pool.BeanstalkPool;

public class ExamplePool {

	protected static Logger log = LoggerFactory.getLogger(ExamplePool.class);

	public static void main(String[] args) {
		pooledExample();
	}

	public static void pooledExample() {
		BeanstalkPool pool = BeanstalkFactory.defaultFactory().pool();

		double producerTime = timedMethod(ExamplePool::pooledParallelProducer, pool);
		System.out.println("Producer time taken: " + producerTime);

		double consumerTime = timedMethod(ExamplePool::pooledParallelConsumer, pool);
		System.out.println("Consumer time taken: " + consumerTime);

		System.out.println("Total time taken: " + (consumerTime + producerTime));
	}

	private static void pooledParallelProducer(final BeanstalkPool pool) {
		IntStream.range(0, 1000).boxed().parallel().forEach(f -> {
			try (Producer producer = pool.getResource().producer()) {
				log.info("Job going to be created: {}", f);
				producer.put(1l, 0, 5000, "this is some data".getBytes());
				log.info("Job created: {}", f);
			}
		});
		try (Producer producer = pool.getResource().producer()) {
			log.info(producer.tubeStats());
		}
		log.info("All Jobs enqueue");
	}

	private static void pooledParallelConsumer(final BeanstalkPool pool) {
		IntStream.range(0, 1000).boxed().parallel().forEach(f -> {
			try (Worker worker = pool.getResource().worker()){
				BeanstalkJob job = worker.reserve(60).get();
				log.info("Got job: " + job);
				worker.delete(job);
			}
		});
		try (Worker worker = pool.getResource().worker()) {
			log.info(worker.tubeStats());
		}

	}

	private static double timedMethod(Consumer<BeanstalkPool> function, BeanstalkPool pool) {
		long time = System.nanoTime();
		function.accept(pool);
		return (System.nanoTime() - time) / 1_000_000.0;
	}

}
