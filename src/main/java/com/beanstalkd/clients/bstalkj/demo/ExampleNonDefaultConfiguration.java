package com.beanstalkd.clients.bstalkj.demo;

//import static com.beanstalkd.clients.bstalkj.BeanstalkFactory.factory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beanstalkd.clients.bstalkj.BeanstalkFactory;
import com.beanstalkd.clients.bstalkj.BeanstalkJob;
import com.beanstalkd.clients.bstalkj.Producer;
import com.beanstalkd.clients.bstalkj.Worker;
import com.beanstalkd.clients.bstalkj.pool.BeanstalkPool;

public class ExampleNonDefaultConfiguration {

    protected static Logger log = LoggerFactory.getLogger(ExampleNonDefaultConfiguration.class);

    public static void main(String[] args) {
        notPooledExample();
        pooledExample();
    }

    public static void notPooledExample() {
        BeanstalkFactory factory = BeanstalkFactory.factory()
                .host("localhost")
                .port(11300)
                .connectionTimeout(5000)
                .tube("dummy_tube")
                .build();

        Producer producer = factory.get().producer();
        producer.put(1l, 0, 5000, "this is some data".getBytes());
        log.info(producer.tubeStats());

        BeanstalkFactory defaultFactory = BeanstalkFactory.factory().build();
        Worker worker = factory.get().worker();
        BeanstalkJob job = worker.reserve(60).get();
        log.info("Got job: " + job);
        worker.delete(job);
        worker.tubeStats();
    }

    public static void pooledExample() {
        BeanstalkFactory factory = BeanstalkFactory.factory()
                .host("localhost")
                .port(11300)
                .connectionTimeout(5000)
                .tube("another_dummy_tube")
                .build();

        // We can create specific configuration for the pool
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(8);
        poolConfig.setMaxIdle(8);

        BeanstalkPool pool = new BeanstalkPool(poolConfig, factory);

        try (Producer producer = pool.getResource().producer()) {
            producer.put(1l, 0, 5000, "this is some data".getBytes());
            log.info(producer.tubeStats());
        }

        try (Worker worker = pool.getResource().worker()) {
            BeanstalkJob job = worker.reserve(60).get();
            log.info("Got job: " + job);
            worker.delete(job);
        }
        pool.destroy();
    }

    public static void pooledExample2() {
        BeanstalkPool pool = BeanstalkFactory.factory().tube("some-tube").build().pool();

        try (Producer producer = pool.getResource().producer()) {
            producer.put(1l, 0, 5000, "this is some data".getBytes());
            log.info(producer.tubeStats());
        }

        try (Worker worker = pool.getResource().worker()) {
            BeanstalkJob job = worker.reserve(60).get();
            log.info("Got job: " + job);
            worker.delete(job);
        }
        pool.destroy();
    }
}
