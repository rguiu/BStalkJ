package com.beanstalkd.clients.bstalkj;

import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.Command.BURY;
import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.Command.DELETE;
import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.Command.IGNORE;
import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.Command.PUT;
import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.Command.RELEASE;
import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.Command.RESERVE;
import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.Command.RESERVE_WITH_TIMEOUT;
import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.Command.STATS_TUBE;
import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.Command.USE;
import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.Command.WATCH;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.util.Optional;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beanstalkd.clients.bstalkj.exceptions.BeanstalkDisconnectedException;
import com.beanstalkd.clients.bstalkj.exceptions.BeanstalkException;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

@ToString(exclude = {"connection"})
public class Client implements Closeable {
    private static Logger log = LoggerFactory.getLogger(Client.class);
    protected BeanstalkConnection connection;
    private String tube;

    private boolean active;

    @Accessors(fluent = true)
    @Setter
    private Consumer<Client> closer;

    public Client(BeanstalkConnection connection, String tube) {
        this(connection, tube, (c) -> {
        });
    }

    public Client(BeanstalkConnection connection, String tube, Consumer<Client> closer) {
        this.connection = connection;
        this.tube = tube;
        this.closer = closer;
    }

    public boolean isActive() {
        return active;
    }

    public Worker worker() {
        return new Worker(this);
    }

    public Producer producer() {
        return new Producer(this);
    }

    protected void useTube(String tube) {
        execute(USE, tube);
    }

    protected int watchTube(String tube) {
        String response = execute(WATCH, tube);
        return Integer.parseInt(response.split(" ")[1]);
    }

    protected int ignoreTube(String tube) {
        String response = execute(IGNORE, tube);
        return Integer.parseInt(response.split(" ")[1]);
    }

    protected String tubeStats() {
        return this.tubeStats(this.tube);
    }

    protected long put(long priority, int delay, int ttr, byte[] data) {
        String response = execute(PUT, data, priority, delay, ttr, data.length);
        return Long.parseLong(response.replaceAll("[^0-9]", ""));
    }

    protected void deleteJob(BeanstalkJob job) throws BeanstalkException {
        deleteJob(job.getId());
    }

    protected Optional<BeanstalkJob> reserve() {
        return processReserveResponse(execute(RESERVE));
    }

    protected Optional<BeanstalkJob> reserve(Integer timeoutSeconds) {
        return processReserveResponse(execute(RESERVE_WITH_TIMEOUT, timeoutSeconds));
    }

    protected void release(BeanstalkJob job, int priority, int delay) {
        release(job.getId(), priority, delay);
    }

    protected void bury(BeanstalkJob job, int priority) {
        execute(BURY, job.getId(), priority);
    }

    protected void init() {
        if (active) {
            return;
        }
        this.active = true;
        this.connection.connect();
        if (this.tube != null && !this.tube.equalsIgnoreCase(BeanstalkProtocol.DEFAULT_TUBE)) {
            this.useTube(tube);
            this.watchTube(tube);
            this.ignoreTube(BeanstalkProtocol.DEFAULT_TUBE);
        }
    }

    public void destroy() {
        if (this.connection != null) {
            this.connection.close();
        }
        this.active = false;
    }

    private void release(long id, int priority, int delay) {
        execute(RELEASE, id, priority, delay);
    }

    private void deleteJob(long id) {
        execute(DELETE, id);
    }

    private String tubeStats(String tube) {
        String response = execute(STATS_TUBE, tube);
        int numBytes = Integer.parseInt(response.split(" ")[1]);
        return new String(connection.readBytes(numBytes));
    }

    private String execute(BeanstalkProtocol.Command command, byte[] data, Object... args) {
        try {
            String commandAsString = command.get(args);
            log.info("Command as String: {}", commandAsString);
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            buf.write(commandAsString.getBytes());
            buf.write(data);
            buf.write("\r\n".getBytes());
            return execute(command, buf.toByteArray());
        } catch (Exception x) {
            throw new BeanstalkException(x);
        }
    }

    private String execute(BeanstalkProtocol.Command command, Object... args) {
        String commandAsString = command.get(args);
        log.info("Command as String: {}", commandAsString);
        return execute(command, commandAsString.getBytes());
    }

    private String execute(BeanstalkProtocol.Command command, byte[] data) {
        try {
            this.init();
            connection.write(data);
            String response = connection.readControlResponse();
            command.check(response);
            return response;
        } catch (BeanstalkDisconnectedException e) {
            this.active = false;
            throw e;
        }
    }

    private Optional<BeanstalkJob> processReserveResponse(String response) {
        if (response.startsWith("TIMED_OUT")) {
            return Optional.empty();
        }
        String[] tmp = response.split("\\s+");
        long id = Long.parseLong(tmp[1]);
        int numBytes = Integer.parseInt(tmp[2]);
        byte[] bytes = connection.readBytes(numBytes);
        return Optional.of(BeanstalkJob.builder().data(bytes).id(id).build());
    }

    @Override
    public void close() {
        closer.accept(this);
    }
}