package com.beanstalkd.clients.bstalkj.pool;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import com.beanstalkd.clients.bstalkj.BeanstalkFactory;
import com.beanstalkd.clients.bstalkj.Client;
import lombok.Builder;

@Builder(builderMethodName = "pooledFactory")
public class BeanstalkPooledFactory implements PooledObjectFactory<Client> {

    private BeanstalkFactory factory;

    @Override
    public void activateObject(PooledObject<Client> pooledConnection) throws Exception {
        if (!pooledConnection.getObject().isActive()) {
            factory.activateObject(pooledConnection.getObject());
        }
    }

    @Override
    public void destroyObject(PooledObject<Client> pooledConnection) throws Exception {
        pooledConnection.getObject().destroy();
    }

    @Override
    public PooledObject<Client> makeObject() throws Exception {
        return new DefaultPooledObject<Client>(factory.get());
    }

    @Override
    public void passivateObject(PooledObject<Client> pooledConnection) throws Exception {
//        pooledConnection.getObject().close();
    }

    @Override
    public boolean validateObject(PooledObject<Client> pooledConnection) {
        try {
            return pooledConnection.getObject().isActive();
        } catch (final Exception e) {
            return false;
        }
    }

    public static BeanstalkPooledFactory defaultPooledFactory() {
        return BeanstalkPooledFactory.pooledFactory().factory(BeanstalkFactory.factory().build()).build();
    }
}
