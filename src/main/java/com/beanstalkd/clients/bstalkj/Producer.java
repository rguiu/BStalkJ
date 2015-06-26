package com.beanstalkd.clients.bstalkj;

import java.io.Closeable;

public class Producer implements Closeable{
    private Client client;

    public Producer(Client client) {
        this.client = client;
    }

    /**
     * Puts a task into the currently used queue.
     * @param priority The job priority, from 0 to 2^32. Most urgent = 0, least urgent = 4_294_967_295.
     * @param delay The time in seconds the server will wait before putting the job on the ready queue.
     * @param ttr The job time-to-run. The server will automatically release the job after this TTR (in seconds)
     *   after a client reserves it. Minimum ttr is 1, if 0 is sent the server will increase to 1.
     * @param data The job data.
     * @return The id of the inserted job.
     */
    public long put(long priority, int delay, int ttr, byte[]data) {
        return client.put(priority, delay, ttr, data);
    }

    /**
     * Subsequent put commands will put jobs into the  tube specified by this command.
     * @param tube the tube to be used
     */
    public void use(String tube) {
        client.useTube(tube);
    }

    /**
     * A YAML response with stats on the current tube
     * @return The stats in YAML format
     */
    public String tubeStats() {
        return client.tubeStats();
    }

    @Override
    public void close() {
        client.close();
    }
}
