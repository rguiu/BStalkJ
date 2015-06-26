package com.beanstalkd.clients.bstalkj;

import java.io.Closeable;
import java.util.Optional;
import com.beanstalkd.clients.bstalkj.exceptions.BeanstalkException;

public class Worker implements Closeable {
    private Client client;

    public Worker(Client client) {
        this.client = client;
    }

    public Optional<BeanstalkJob> reserve() {
        return client.reserve();
    }

    public Optional<BeanstalkJob> reserve(int timeout) {
        return client.reserve(timeout);
    }

    public void deadlineSoon() {
        throw new BeanstalkException("Not implemented yet");
    }

    public void timeout() {
        throw new BeanstalkException("Not implemented yet");
    }

    public void reserved() {
        throw new BeanstalkException("Not implemented yet");
    }

    public void delete(BeanstalkJob job) {
        client.deleteJob(job);
    }

    /**
     * Releases a job (places it back onto the queue).
     *
     * @param job      The job to release. This job must previously have been reserved.
     * @param priority The new priority to assign to the released job.
     * @param delay    The number of seconds the server should wait before placing the job onto the ready queue.
     */
    public void release(BeanstalkJob job, int priority, int delay) {
        client.release(job, priority, delay);
    }

    /**
     * Buries a job ("buried" state means the job will not be touched by the server again until "kicked").
     *
     * @param job      The job to bury. This job must previously have been reserved.
     * @param priority The new priority to assign to the job.
     * @throws BeanstalkException If an unexpected response is received from the server, or other unexpected
     *                            problem occurs.
     */
    public void bury(BeanstalkJob job, int priority) {
        client.bury(job, priority);
    }

    public void touch(BeanstalkJob job) {
        throw new BeanstalkException("Not implemented yet");
    }

    public int watch(String tube) {
        return client.watchTube(tube);
    }

    public int ignore(String tube) {
        return client.ignoreTube(tube);
    }

    public String tubeStats() {
        return client.tubeStats();
    }

    @Override
    public void close() {
        client.close();
    }
}
