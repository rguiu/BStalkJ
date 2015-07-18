package com.beanstalkd.clients.bstalkj;

import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.DEFAULT_HOST;
import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.DEFAULT_PORT;
import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.DEFAULT_SO_TIMEOUT;
import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.DEFAULT_TIMEOUT;
import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.DEFAULT_TUBE;
import java.util.function.Consumer;
import com.beanstalkd.clients.bstalkj.pool.BeanstalkPool;
import lombok.Builder;
import lombok.ToString;

@Builder
@ToString
public class BeanstalkFactory {
    private String host;
    private int port;
    private int connectionTimeout;
    private int soTimeout;
    private String tube;

    public static BeanstalkFactoryBuilder factory() {
        return BeanstalkFactory.builder()
                               .host(DEFAULT_HOST)
                               .connectionTimeout(DEFAULT_TIMEOUT)
                               .port(DEFAULT_PORT)
                               .soTimeout(DEFAULT_SO_TIMEOUT)
                               .tube(DEFAULT_TUBE);
    }

    public Client get() {
        return new Client(connection(), this.tube);
    }

    public Client get(Consumer<Client> consumer) {
        return new Client(connection(), this.tube, consumer);
    }

    public void activateObject(Client client) {
        client.init();
    }

    public BeanstalkPool pool() {
        return new BeanstalkPool(this);
    }

    private BeanstalkConnection connection() {
        return BeanstalkConnection.connection()
                                  .host(host)
                                  .port(port)
                                  .soTimeout(soTimeout)
                                  .connectionTimeout(connectionTimeout)
                                  .build();
    }
}
