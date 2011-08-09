package com.hellblazer.pinkie;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

public class TestServerSocketChannelHandler extends TestCase {

    public void testAccept() throws Exception {
        final SimpleServerSocketChannelHandler handler = new SimpleServerSocketChannelHandler(
                                                                                              "Test Handler",
                                                                                              new SocketOptions(),
                                                                                              new InetSocketAddress(
                                                                                                                    0),
                                                                                              Executors.newSingleThreadExecutor());
        handler.start();
        InetSocketAddress endpont = handler.getLocalAddress();
        SocketChannel outbound = SocketChannel.open();
        outbound.configureBlocking(true);
        outbound.connect(endpont);
        assertTrue(outbound.finishConnect());
        waitFor("No handler was created", new Condition() {
            @Override
            public boolean value() {
                return handler.handlers.size() >= 1;
            }
        }, 2000, 100);
        assertEquals(1, handler.handlers.size());
    }

    public void testRead() throws Exception {
        final SimpleServerSocketChannelHandler handler = new SimpleServerSocketChannelHandler(
                                                                                              "Test Handler",
                                                                                              new SocketOptions(),
                                                                                              new InetSocketAddress(
                                                                                                                    0),
                                                                                              Executors.newSingleThreadExecutor());
        handler.start();
        InetSocketAddress endpont = handler.getLocalAddress();
        SocketChannel outbound = SocketChannel.open();
        outbound.configureBlocking(true);
        outbound.connect(endpont);
        outbound.finishConnect();
        waitFor("No handler was created", new Condition() {
            @Override
            public boolean value() {
                return handler.handlers.size() >= 1;
            }
        }, 2000, 100);
        final SimpleSocketChannelHandler scHandler = handler.handlers.get(0);
        scHandler.selectForRead();
        ByteBuffer buf = ByteBuffer.wrap(new byte[512]);
        byte[] src = new byte[512];
        Arrays.fill(src, (byte) 6);
        buf.put(src);
        buf.flip();
        int written = 0;
        for (int out = outbound.write(buf); written + out == src.length; written += out)
            ;
        waitFor("No read was recorded", new Condition() {
            @Override
            public boolean value() {
                return scHandler.reads.size() >= 1;
            }
        }, 2000, 100);
        assertEquals(1, scHandler.reads.size());
        assertEquals(src.length, scHandler.reads.get(0).length);
        for (int i = 0; i < src.length; i++) {
            assertEquals(src[i], scHandler.reads.get(0)[i]);
        }

        buf = ByteBuffer.wrap(new byte[512]);
        src = new byte[512];
        Arrays.fill(src, (byte) 12);
        buf.put(src);
        buf.flip();
        written = 0;
        for (int out = outbound.write(buf); written + out == src.length; written += out)
            ;
        waitFor("No further read was recorded", new Condition() {
            @Override
            public boolean value() {
                return scHandler.reads.size() >= 2;
            }
        }, 2000, 100);
        assertEquals(2, scHandler.reads.size());
        assertEquals(src.length, scHandler.reads.get(1).length);
        for (int i = 0; i < src.length; i++) {
            assertEquals(src[i], scHandler.reads.get(1)[i]);
        }
    }

    public void testWrite() throws Exception {
        final SimpleServerSocketChannelHandler handler = new SimpleServerSocketChannelHandler(
                                                                                              "Test Handler",
                                                                                              new SocketOptions(),
                                                                                              new InetSocketAddress(
                                                                                                                    0),
                                                                                              Executors.newSingleThreadExecutor());
        handler.start();
        InetSocketAddress endpont = handler.getLocalAddress();
        final SocketChannel outbound = SocketChannel.open();
        outbound.configureBlocking(true);
        outbound.connect(endpont);
        outbound.finishConnect();
        waitFor("No handler was created", new Condition() {
            @Override
            public boolean value() {
                return handler.handlers.size() >= 1;
            }
        }, 2000, 100);
        final SimpleSocketChannelHandler scHandler = handler.handlers.get(0);
        scHandler.selectForWrite();
        byte[][] src = new byte[2][];

        ByteBuffer buf = ByteBuffer.wrap(new byte[512]);
        src[0] = new byte[512];
        Arrays.fill(src[0], (byte) 6);
        buf.put(src[0]);
        buf.flip();
        scHandler.writes.add(buf);

        buf = ByteBuffer.wrap(new byte[512]);
        src[1] = new byte[512];
        Arrays.fill(src[1], (byte) 12);
        buf.put(src[1]);
        buf.flip();
        scHandler.writes.add(buf);

        scHandler.selectForWrite();

        buf = ByteBuffer.wrap(new byte[512]);
        final ByteBuffer finalBuf = buf;
        waitFor("Write was not completed", new Condition() {
            @Override
            public boolean value() {
                try {
                    for (int read = outbound.read(finalBuf); read != 0; read = outbound.read(finalBuf))
                        ;
                } catch (IOException e) {
                    fail("Exception during read: " + e.toString());
                }
                return scHandler.writes.size() <= 1;
            }
        }, 2000, 100);
        buf.flip();
        assertEquals(src[0].length, buf.limit());
        for (int i = 0; i < src[0].length; i++) {
            assertEquals(src[0][i], buf.get(i));
        }

        buf = ByteBuffer.wrap(new byte[512]);
        final ByteBuffer finalBuffy = buf;
        waitFor("Write was not completed", new Condition() {
            @Override
            public boolean value() {
                try {
                    for (int read = outbound.read(finalBuffy); read != 0; read = outbound.read(finalBuffy))
                        ;
                } catch (IOException e) {
                    fail("Exception during read: " + e.toString());
                }
                return scHandler.writes.size() == 0;
            }
        }, 2000, 100);
        buf.flip();
        assertEquals(src[1].length, buf.limit());
        for (int i = 0; i < src[1].length; i++) {
            assertEquals(src[1][i], buf.get(i));
        }
    }

    void waitFor(String reason, Condition condition, long timeout, long interval)
                                                                                 throws InterruptedException {
        long target = System.currentTimeMillis() + timeout;
        while (!condition.value()) {
            if (target < System.currentTimeMillis()) {
                fail(reason);
            }
            Thread.sleep(interval);
        }
    }

    private static interface Condition {
        boolean value();
    }
}
