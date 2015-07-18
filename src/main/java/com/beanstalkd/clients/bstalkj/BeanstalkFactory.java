package com.beanstalkd.clients.bstalkj;

import java.util.function.Consumer;
import com.beanstalkd.clients.bstalkj.pool.BeanstalkPool;
import lombok.Builder;

@Builder(builderMethodName = "factory")
public class BeanstalkFactory {
    private String host = BeanstalkProtocol.DEFAULT_HOST;
    private int port = BeanstalkProtocol.DEFAULT_PORT;
    private int connectionTimeout = BeanstalkProtocol.DEFAULT_TIMEOUT;
    private int soTimeout = BeanstalkProtocol.DEFAULT_SO_TIMEOUT;
    private String tube = BeanstalkProtocol.DEFAULT_TUBE;

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
