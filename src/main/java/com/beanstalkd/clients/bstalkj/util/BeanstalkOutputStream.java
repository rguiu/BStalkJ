package com.beanstalkd.clients.bstalkj. util;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public final class BeanstalkOutputStream extends FilterOutputStream {
        protected final byte buf[];

        protected int count;

        public BeanstalkOutputStream(final OutputStream out) {
            this(out, 8192);
        }

        public BeanstalkOutputStream(final OutputStream out, final int size) {
            super(out);
            if (size <= 0) {
                throw new IllegalArgumentException("Buffer size <= 0");
            }
            buf = new byte[size];
        }

        private void flushBuffer() throws IOException {
            if (count > 0) {
                out.write(buf, 0, count);
                count = 0;
            }
        }

        public void write(final byte b) throws IOException {
            if (count == buf.length) {
                flushBuffer();
            }
            buf[count++] = b;
        }

        public void write(final byte[] b) throws IOException {
            write(b, 0, b.length);
        }

        public void write(final byte b[], final int off, final int len) throws IOException {
            if (len >= buf.length) {
                flushBuffer();
                out.write(b, off, len);
            } else {
                if (len >= buf.length - count) {
                    flushBuffer();
                }

                System.arraycopy(b, off, buf, count, len);
                count += len;
            }
        }

        public void flush() throws IOException {
            flushBuffer();
            out.flush();
        }
    }