##a Java beanstalkd client
This is a work in progress. Client library for beanstalkd.

###Usage:

####Non pooled connection
Example of a **producer**:

```java
// Create a factory with given configuration
BeanstalkFactory factory = BeanstalkFactory.builder().host("localhost")
												     .port(11300)
												     .connectionTimeout(5000)
												     .tube("dummy_tube")
												     .build();
// fetch the producer
Producer producer = factory.get().producer();
// place job
producer.put(1l, 0, 5000, "this is some data".getBytes());
// print tube stats
System.out.println(producer.tubeStats());
```
To see more on the [Producer](/src/main/java/com/beanstalkd/clients/bstalkj/Producer.java)

Example of a **worker/consumer**:

```java
// We can use the default factory, poiting to localhost:11300
BeanstalkFactory factory = BeanstalkFactory.defaultFactory();

Worker worker = factory.get().worker();
BeanstalkJob job = worker.reserve(60).get();

System.out.println("Got job: " + job);

worker.delete(job);
System.out.println(worker.tubeStats());
```
To see more on the [Consumer](/src/main/java/com/beanstalkd/clients/bstalkj/Consumer.java)

####Pooled connection
Example of a **producer**:

```java
// Like in non-pooled client we need to configure first or connection
BeanstalkFactory factory = BeanstalkFactory.factory().host("localhost")
												     .port(11300)
												     .connectionTimeout(5000)
												     .tube("another_dummy_tube")
												     .build();

// Optionally we could configure non-default pool parameters
GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
poolConfig.setMaxTotal(8);
poolConfig.setMaxIdle(8);

BeanstalkPool pool = new BeanstalkPool(poolConfig, factory);
// If we use default pool configuration we could do:
// BeanstalkPool pool = new BeanstalkPool(factory);
					
try (Producer producer = pool.getResource().producer()) {
	producer.put(1l, 0, 5000, "this is some data".getBytes());
	log.info(producer.tubeStats());
}						 												 
```

Using try-with-resource we ensure the client is returned to the pool.

And for the **Worker**:

```java
try (Worker worker = pool.getResource().worker()) {
			BeanstalkJob job = worker.reserve(60).get();
			log.info("Got job: " + job);
			worker.delete(job);
}
```

Another fast way of creating a pool in a single line but the tube itself can be:

```
BeanstalkPool pool = BeanstalkFactory.factory()
                                     .host("xyz.blah.com")
                                     .port(1111)
                                     .tube("some-tube")
                                     .build()
                                     .pool();
```


###Credits

+ I have taken ideas and code from: [Jedis](https://github.com/xetorthio/jedis)

+ And from: [TrendrrBeanstalk](https://github.com/dustismo/TrendrrBeanstalk)

License is MIT

