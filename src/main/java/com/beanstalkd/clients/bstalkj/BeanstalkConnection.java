package com.beanstalkd.clients.bstalkj;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.NotYetConnectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beanstalkd.clients.bstalkj.exceptions.BeanstalkDisconnectedException;
import com.beanstalkd.clients.bstalkj.exceptions.BeanstalkException;
import com.beanstalkd.clients.bstalkj.util.BeanstalkInputStream;
import com.beanstalkd.clients.bstalkj.util.BeanstalkOutputStream;
import lombok.Builder;

@Builder(builderMethodName = "connection")
public class BeanstalkConnection implements Closeable {
    private final static Logger log = LoggerFactory.getLogger(BeanstalkConnection.class);

    private Socket socket;
    private BeanstalkInputStream inputStream;
    private BeanstalkOutputStream outputStream;

    private String host;
    private int port;
    private int connectionTimeout;
    private int soTimeout;

    private boolean broken;

    public void connect() {
        if (isOpen()) {
            throw new BeanstalkException("Already connected");
        }
        reset();
    }

    @Override
    public void close() {
        if (isOpen()) {
            try {
                outputStream.flush();
                socket.close();
            } catch (IOException ex) {
                broken = true;
                throw new BeanstalkException(ex);
            } finally {
                BeanstalkConnection.closeQuietly(socket);
            }
        }
    }

    public boolean isOpen() {
        return socket != null && socket.isBound() && !socket.isClosed() && socket.isConnected()
                && !socket.isInputShutdown() && !socket.isOutputShutdown();
    }

    public void write(String str) throws BeanstalkException {
        try {
            outputStream.write(str.getBytes());
            outputStream.flush();
            log.debug("Wrote: {}", str);
        } catch (IOException x) {
            this.throwException(x);
        }
    }

    private void throwException(Exception x) throws BeanstalkException {
        if (x instanceof NotYetConnectedException) {
            throw new BeanstalkDisconnectedException(x);
        }
        if (x instanceof IOException) {
            throw new BeanstalkDisconnectedException(x);
        }
        throw new BeanstalkException(x);
    }

    public void write(byte[] bytes) {
        try {
            outputStream.write(bytes);
            outputStream.flush();
        } catch (Exception x) {
            this.throwException(x);
        }
    }

    /**
     * returns the control response.  ends with \r\n
     *
     * @return String
     */
    public String readControlResponse() throws BeanstalkException {
        byte[] bytes = inputStream.readLineBytes();
        while (bytes.length == 0) { // @Todo, getting one empty line in some cases. Find out why. (DELETE)
            bytes = inputStream.readLineBytes();
        }
        return new String(bytes);
    }

    public byte[] readBytes(int numBytes) throws BeanstalkException {
        byte[] bytes = new byte[numBytes];
        int bytesRead = 0;
        while (bytesRead < numBytes) {
            bytesRead += this.inputStream.read(bytes, bytesRead, numBytes);
        }
        return bytes;
    }

    private static void closeQuietly(Socket sock) {
        if (sock != null) {
            try {
                sock.close();
            } catch (IOException e) {
                // ignored
            }
        }
    }

    private void reset() {
        try {
            socket = new Socket();
            socket.setReuseAddress(true);
            socket.setKeepAlive(true);
            socket.setTcpNoDelay(true);
            socket.setSoLinger(true, 0);
            socket.connect(new InetSocketAddress(this.host, this.port), this.connectionTimeout);
            socket.setSoTimeout(this.soTimeout);

            this.inputStream = new BeanstalkInputStream(socket.getInputStream());
            this.outputStream = new BeanstalkOutputStream(socket.getOutputStream());

        } catch (IOException ex) {
            broken = true;
            throw new BeanstalkException(ex);
        }
    }

}
