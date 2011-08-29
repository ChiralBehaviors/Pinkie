/*
 * Copyright (c) 2009, 2011 Hal Hildebrand, all rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hellblazer.pinkie;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

/**
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class TestServerSocketChannelHandler extends TestCase {

    private static interface Condition {
        boolean value();
    }

    public void testAccept() throws Exception {
        final SimpleCommHandlerFactory factory = new SimpleCommHandlerFactory();
        final ServerSocketChannelHandler<SimpleCommHandler> handler = new ServerSocketChannelHandler<SimpleCommHandler>(
                                                                                                                        "Test Handler",
                                                                                                                        new SocketOptions(),
                                                                                                                        new InetSocketAddress(
                                                                                                                                              0),
                                                                                                                        Executors.newSingleThreadExecutor(),
                                                                                                                        factory);
        handler.start();
        InetSocketAddress endpont = handler.getLocalAddress();
        SocketChannel outbound = SocketChannel.open();
        outbound.configureBlocking(true);
        outbound.connect(endpont);
        assertTrue(outbound.finishConnect());
        waitFor("No handler was created", new Condition() {
            @Override
            public boolean value() {
                return factory.handlers.size() >= 1;
            }
        }, 2000, 100);
        final SimpleCommHandler scHandler = factory.handlers.get(0);
        waitFor("Handler was not accepted", new Condition() {
            @Override
            public boolean value() {
                return scHandler.accepted.get();
            }
        }, 2000, 100);
        assertEquals(1, factory.handlers.size());
    }

    public void testRead() throws Exception {
        final SimpleCommHandlerFactory factory = new SimpleCommHandlerFactory();
        final ServerSocketChannelHandler<SimpleCommHandler> handler = new ServerSocketChannelHandler<SimpleCommHandler>(
                                                                                                                        "Test Handler",
                                                                                                                        new SocketOptions(),
                                                                                                                        new InetSocketAddress(
                                                                                                                                              0),
                                                                                                                        Executors.newSingleThreadExecutor(),
                                                                                                                        factory);
        handler.start();
        InetSocketAddress endpont = handler.getLocalAddress();
        SocketChannel outbound = SocketChannel.open();
        outbound.configureBlocking(true);
        outbound.connect(endpont);
        outbound.finishConnect();
        waitFor("No handler was created", new Condition() {
            @Override
            public boolean value() {
                return factory.handlers.size() >= 1;
            }
        }, 2000, 100);
        final SimpleCommHandler scHandler = factory.handlers.get(0);
        waitFor("Handler was not accepted", new Condition() {
            @Override
            public boolean value() {
                return scHandler.accepted.get();
            }
        }, 2000, 100);
        scHandler.selectForRead();
        ByteBuffer buf = ByteBuffer.wrap(new byte[512]);
        byte[] src = new byte[512];
        Arrays.fill(src, (byte) 6);
        buf.put(src);
        buf.flip();
        int written = 0;
        for (int out = outbound.write(buf); written + out == src.length; written += out) {
            ;
        }
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
        for (int out = outbound.write(buf); written + out == src.length; written += out) {
            ;
        }
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
        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setSend_buffer_size(128);
        socketOptions.setReceive_buffer_size(128);
        socketOptions.setTimeout(100);
        final SimpleCommHandlerFactory factory = new SimpleCommHandlerFactory();
        final ServerSocketChannelHandler<SimpleCommHandler> handler = new ServerSocketChannelHandler<SimpleCommHandler>(
                                                                                                                        "Test Handler",
                                                                                                                        new SocketOptions(),
                                                                                                                        new InetSocketAddress(
                                                                                                                                              0),
                                                                                                                        Executors.newSingleThreadExecutor(),
                                                                                                                        factory);
        handler.start();
        InetSocketAddress endpont = handler.getLocalAddress();
        final SocketChannel outbound = SocketChannel.open();
        socketOptions.configure(outbound.socket());
        outbound.configureBlocking(true);
        outbound.connect(endpont);
        outbound.finishConnect();
        waitFor("No handler was created", new Condition() {
            @Override
            public boolean value() {
                return factory.handlers.size() >= 1;
            }
        }, 2000, 100);
        outbound.configureBlocking(true);
        final SimpleCommHandler scHandler = factory.handlers.get(0);
        waitFor("Handler was not accepted", new Condition() {
            @Override
            public boolean value() {
                return scHandler.accepted.get();
            }
        }, 2000, 100);
        scHandler.selectForWrite();
        final byte[][] src = new byte[2][];

        ByteBuffer buf = ByteBuffer.wrap(new byte[8192]);
        src[0] = new byte[8192];
        Arrays.fill(src[0], (byte) 6);
        buf.put(src[0]);
        buf.flip();
        scHandler.writes.add(buf);

        buf = ByteBuffer.wrap(new byte[8192]);
        src[1] = new byte[8192];
        Arrays.fill(src[1], (byte) 12);
        buf.put(src[1]);
        buf.flip();
        scHandler.writes.add(buf);

        scHandler.selectForWrite();

        final ByteArrayOutputStream testBuf = new ByteArrayOutputStream();
        final ByteBuffer readBuf = ByteBuffer.wrap(new byte[512]);
        readBuf.clear();
        waitFor("Write was not completed", new Condition() {
            @Override
            public boolean value() {
                try {
                    int read = outbound.read(readBuf);
                    byte[] anotherBuf = new byte[read];
                    readBuf.flip();
                    readBuf.get(anotherBuf);
                    testBuf.write(anotherBuf);
                    readBuf.clear();
                } catch (IOException e) {
                    fail("Exception during read: " + e.toString());
                }
                return src[0].length == testBuf.size();
            }
        }, 4000, 100);
        byte[] testArray = testBuf.toByteArray();
        for (int i = 0; i < src[0].length; i++) {
            assertEquals(src[0][i], testArray[i]);
        }

        scHandler.selectForWrite();
        testBuf.reset();
        readBuf.clear();
        waitFor("Write was not completed", new Condition() {
            @Override
            public boolean value() {
                try {
                    int read = outbound.read(readBuf);
                    byte[] anotherBuf = new byte[read];
                    readBuf.flip();
                    readBuf.get(anotherBuf);
                    testBuf.write(anotherBuf);
                    readBuf.clear();
                } catch (IOException e) {
                    fail("Exception during read: " + e.toString());
                }
                return src[0].length == testBuf.size();
            }
        }, 4000, 100);
        testArray = testBuf.toByteArray();
        for (int i = 0; i < src[1].length; i++) {
            assertEquals(src[1][i], testArray[i]);
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
}
