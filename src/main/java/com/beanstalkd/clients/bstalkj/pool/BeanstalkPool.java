package com.beanstalkd.clients.bstalkj.pool;

import java.io.Closeable;
import java.util.function.Consumer;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import com.beanstalkd.clients.bstalkj.BeanstalkFactory;
import com.beanstalkd.clients.bstalkj.Client;
import com.beanstalkd.clients.bstalkj.exceptions.BeanstalkException;

public class BeanstalkPool implements Closeable {
    protected GenericObjectPool<Client> internalPool;

    private Consumer<Client> closer = (client) -> {
        if (client.isActive()) {
            this.returnResource(client);
        } else {
            this.returnBrokenResource(client);
        }
    };

    public BeanstalkPool(BeanstalkFactory factory) {
        this(new GenericObjectPoolConfig(), factory);
    }

    public BeanstalkPool(final GenericObjectPoolConfig poolConfig, BeanstalkFactory factory) {
        PooledObjectFactory<Client> pooledFactory = BeanstalkPooledFactory.pooledFactory().factory(factory).build();
        initPool(poolConfig, pooledFactory);
    }

    @Override
    public void close() {
        closeInternalPool();
    }

    public boolean isClosed() {
        return this.internalPool.isClosed();
    }

    public void initPool(final GenericObjectPoolConfig poolConfig, PooledObjectFactory<Client> factory) {
        if (this.internalPool != null) {
            try {
                closeInternalPool();
            } catch (Exception e) {
                //ignore
            }
        }
        this.internalPool = new GenericObjectPool<Client>(factory, poolConfig);
    }

    public Client getResource() {
        try {
            return internalPool.borrowObject().closer(closer);
        } catch (Exception e) {
            throw new BeanstalkException("Could not get a resource from the pool", e);
        }
    }

    protected void returnResourceObject(final Client resource) {
        if (resource == null) {
            return;
        }
        try {
            internalPool.returnObject(resource);
        } catch (Exception e) {
            throw new BeanstalkException("Could not return the resource to the pool", e);
        }
    }

    public void returnBrokenResource(final Client resource) {
        if (resource != null) {
            returnBrokenResourceObject(resource);
        }
    }

    public void returnResource(final Client resource) {
        if (resource != null) {
            returnResourceObject(resource);
        }
    }

    public void destroy() {
        closeInternalPool();
    }

    protected void returnBrokenResourceObject(final Client resource) {
        try {
            internalPool.invalidateObject(resource);
        } catch (Exception e) {
            throw new BeanstalkException("Could not return the resource to the pool", e);
        }
    }

    protected void closeInternalPool() {
        try {
            internalPool.close();
        } catch (Exception e) {
            throw new BeanstalkException("Could not destroy the pool", e);
        }
    }

    public int getNumActive() {
        if (this.internalPool == null || this.internalPool.isClosed()) {
            return -1;
        }

        return this.internalPool.getNumActive();
    }

    public int getNumIdle() {
        if (this.internalPool == null || this.internalPool.isClosed()) {
            return -1;
        }

        return this.internalPool.getNumIdle();
    }

    public int getNumWaiters() {
        if (this.internalPool == null || this.internalPool.isClosed()) {
            return -1;
        }

        return this.internalPool.getNumWaiters();
    }

    public void addObjects(int count) {
        try {
            for (int i = 0; i < count ; i++) {
                this.internalPool.addObject();
            }
        } catch (Exception e) {
            throw new BeanstalkException("Error trying to add idle objects", e);
        }
    }
}
