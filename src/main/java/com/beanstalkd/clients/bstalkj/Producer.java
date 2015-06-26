package com.beanstalkd.clients.bstalkj;

import java.io.Closeable;

public class Producer implements Closeable{
    private Client client;

    public Producer(Client client) {
        this.client = client;
    }

    /**
     * Puts a task into the currently used queue (see {@link #useTube(String)}.
     * @param priority The job priority, from 0 to 2^32. Most urgent = 0, least urgent = 4294967295.
     * @param delay The time the server will wa	it before putting the job on the ready queue.
     * @param ttr The job time-to-run. The server will automatically release the job after this TTR (in seconds)
     *   after a client reserves it.
     * @param data The job data.
     * @return The id of the inserted job.
     */
    public long put(long priority, int delay, int ttr, byte[]data) {
        return client.put(priority, delay, ttr, data);
    }

    public void use(String tube) {
        client.useTube(tube);
    }

    public String tubeStats() {
        return client.tubeStats();
    }

    @Override
    public void close() {
        client.close();
    }
}
