package com.hellblazer.pinkie;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

import junit.framework.TestCase;

public class TestServerSocketChannelHandler extends TestCase {

    public void testAccept() throws Exception {
        final SimpleServerSocketChannelHandler handler = new SimpleServerSocketChannelHandler(
                                                                                              "Test Handler",
                                                                                              new SocketOptions(),
                                                                                              new InetSocketAddress(
                                                                                                                    0),
                                                                                              new ImmediateExecutor());
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
                                                                                              new ImmediateExecutor());
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
