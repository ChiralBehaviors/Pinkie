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

import static com.hellblazer.utils.Utils.waitForCondition;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import org.junit.Test;

import com.hellblazer.utils.Condition;

/**
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class TestServerSocketChannelHandler extends TestCase {
    private static class ReadHandler implements CommunicationsHandler {
        final AtomicBoolean  accepted = new AtomicBoolean();
        SocketChannelHandler handler;
        final ByteBuffer     read;
        final int            readLength;
        List<byte[]>         reads    = new CopyOnWriteArrayList<byte[]>();

        public ReadHandler(int readLength) {
            read = ByteBuffer.allocate(readLength);
            this.readLength = readLength;
        }

        @Override
        public void accept(SocketChannelHandler handler) {
            this.handler = handler;
            accepted.set(true);
        }

        @Override
        public void closing() {
        }

        @Override
        public void connect(SocketChannelHandler handler) {
        }

        @Override
        public void readReady() {
            try {
                handler.getChannel().read(read);
            } catch (IOException e) {
                throw new IllegalStateException();
            }
            if (!read.hasRemaining()) {
                byte[] buf = new byte[readLength];
                read.flip();
                read.get(buf);
                reads.add(buf);
                read.clear();
            } else {
                handler.selectForRead();
            }
        }

        public void selectForRead() {
            handler.selectForRead();
        }

        @Override
        public void writeReady() {
        }

    }

    private static class ReadHandlerFactory implements
            CommunicationsHandlerFactory {
        List<ReadHandler> handlers = new ArrayList<ReadHandler>();
        final int         readLength;

        public ReadHandlerFactory(int readLength) {
            super();
            this.readLength = readLength;
        }

        @Override
        public CommunicationsHandler createCommunicationsHandler(SocketChannel channel) {
            ReadHandler readHandler = new ReadHandler(readLength);
            handlers.add(readHandler);
            return readHandler;
        }

    }

    public void testAccept() throws Exception {
        final SimpleCommHandlerFactory factory = new SimpleCommHandlerFactory();
        final ServerSocketChannelHandler handler = new ServerSocketChannelHandler(
                                                                                  "Test Handler",
                                                                                  new SocketOptions(),
                                                                                  new InetSocketAddress(
                                                                                                        "127.0.0.1",
                                                                                                        0),
                                                                                  Executors.newCachedThreadPool(),
                                                                                  factory);
        handler.start();
        InetSocketAddress endpont = handler.getLocalAddress();
        SocketChannel outbound = SocketChannel.open();
        outbound.configureBlocking(true);
        outbound.connect(endpont);
        assertTrue(outbound.finishConnect());
        assertTrue(waitForCondition(2000, 100, new Condition() {
            @Override
            public boolean isTrue() {
                return factory.handlers.size() >= 1;
            }
        }));
        final SimpleCommHandler scHandler = factory.handlers.get(0);
        assertTrue(waitForCondition(2000, 100, new Condition() {
            @Override
            public boolean isTrue() {
                return scHandler.accepted.get();
            }
        }));
        assertEquals(1, factory.handlers.size());
        handler.terminate();
    }

    @Test
    public void testCloseBehavior() throws Exception {
        final SimpleCommHandlerFactory factory = new SimpleCommHandlerFactory();
        SocketOptions socketOptions = new SocketOptions();
        final ServerSocketChannelHandler handler = new ServerSocketChannelHandler(
                                                                                  "Test Handler",
                                                                                  socketOptions,
                                                                                  new InetSocketAddress(
                                                                                                        "127.0.0.1",
                                                                                                        0),
                                                                                  Executors.newCachedThreadPool(),
                                                                                  factory);
        handler.start();
        InetSocketAddress endpont = handler.getLocalAddress();
        SocketChannel outbound = SocketChannel.open();
        outbound.configureBlocking(true);
        outbound.connect(endpont);
        assertTrue(outbound.finishConnect());
        assertTrue(waitForCondition(2000, 100, new Condition() {
            @Override
            public boolean isTrue() {
                return factory.handlers.size() >= 1;
            }
        }));
        final SimpleCommHandler scHandler = factory.handlers.get(0);
        assertTrue(waitForCondition(2000, 100, new Condition() {
            @Override
            public boolean isTrue() {
                return scHandler.accepted.get();
            }
        }));
        assertEquals(1, factory.handlers.size());
        scHandler.selectForRead();
        outbound.close();
        assertTrue(waitForCondition(1000, 100, new Condition() {
            @Override
            public boolean isTrue() {
                return scHandler.closed.get();
            }
        }));
        handler.terminate();
    }

    public void testEndToEnd() throws Exception {
        SocketOptions socketOptions = new SocketOptions();
        int bufferSize = 1 * 1024;
        socketOptions.setSend_buffer_size(bufferSize);
        socketOptions.setReceive_buffer_size(128);
        socketOptions.setTimeout(100);
        final SimpleCommHandlerFactory outboundFactory = new SimpleCommHandlerFactory();
        final ServerSocketChannelHandler outboundHandler = new ServerSocketChannelHandler(
                                                                                          "Test write Handler",
                                                                                          socketOptions,
                                                                                          new InetSocketAddress(
                                                                                                                "127.0.0.1",
                                                                                                                0),
                                                                                          Executors.newCachedThreadPool(),
                                                                                          outboundFactory);
        outboundHandler.start();

        int testLength = 8 * 1024;
        final ReadHandlerFactory inboundFactory = new ReadHandlerFactory(
                                                                         testLength);
        final ServerSocketChannelHandler inboundHandler = new ServerSocketChannelHandler(
                                                                                         "Test read Handler",
                                                                                         socketOptions,
                                                                                         new InetSocketAddress(
                                                                                                               "127.0.0.1",
                                                                                                               0),
                                                                                         Executors.newCachedThreadPool(),
                                                                                         inboundFactory);
        InetSocketAddress endpoint = inboundHandler.getLocalAddress();

        inboundHandler.start();

        final SimpleCommHandler writeHandler = new SimpleCommHandler();
        outboundHandler.connectTo(endpoint, writeHandler);

        assertTrue(waitForCondition(1000, 100, new Condition() {
            @Override
            public boolean isTrue() {
                return writeHandler.connected.get();
            }
        }));

        assertTrue(waitForCondition(1000, 100, new Condition() {
            @Override
            public boolean isTrue() {
                return inboundFactory.handlers.size() >= 1;
            }
        }));

        final ReadHandler readHandler = inboundFactory.handlers.get(0);

        assertTrue(waitForCondition(2000, 100, new Condition() {
            @Override
            public boolean isTrue() {
                return readHandler.accepted.get();
            }
        }));

        final byte[][] src = new byte[2][];

        ByteBuffer buf = ByteBuffer.wrap(new byte[testLength]);
        src[0] = new byte[8192];
        Arrays.fill(src[0], (byte) 6);
        buf.put(src[0]);
        buf.flip();
        writeHandler.writes.add(buf);

        buf = ByteBuffer.wrap(new byte[8192]);
        src[1] = new byte[8192];
        Arrays.fill(src[1], (byte) 12);
        buf.put(src[1]);
        buf.flip();
        writeHandler.writes.add(buf);

        writeHandler.selectForWrite();
        readHandler.selectForRead();

        assertTrue(waitForCondition(1000, 100, new Condition() {
            @Override
            public boolean isTrue() {
                return readHandler.reads.size() == 1;
            }
        }));

        writeHandler.selectForWrite();
        readHandler.selectForRead();

        assertTrue(waitForCondition(1000, 100, new Condition() {
            @Override
            public boolean isTrue() {
                return readHandler.reads.size() >= 1;
            }
        }));

        int j = 0;
        for (byte[] result : readHandler.reads) {
            for (int i = 0; i < src[j].length; i++) {
                assertEquals(src[j][i], result[i]);
            }
            j++;
        }
        inboundHandler.terminate();
        outboundHandler.terminate();
    }

    public void testRead() throws Exception {
        final SimpleCommHandlerFactory factory = new SimpleCommHandlerFactory();
        final ServerSocketChannelHandler handler = new ServerSocketChannelHandler(
                                                                                  "Test Handler",
                                                                                  new SocketOptions(),
                                                                                  new InetSocketAddress(
                                                                                                        "127.0.0.1",
                                                                                                        0),
                                                                                  Executors.newCachedThreadPool(),
                                                                                  factory);
        handler.start();
        InetSocketAddress endpont = handler.getLocalAddress();
        SocketChannel outbound = SocketChannel.open();
        outbound.configureBlocking(true);
        outbound.connect(endpont);
        outbound.finishConnect();
        assertTrue(waitForCondition(4000, 100, new Condition() {
            @Override
            public boolean isTrue() {
                return factory.handlers.size() >= 1;
            }
        }));
        final SimpleCommHandler scHandler = factory.handlers.get(0);
        assertTrue(waitForCondition(4000, 100, new Condition() {
            @Override
            public boolean isTrue() {
                return scHandler.accepted.get();
            }
        }));
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
        assertTrue(waitForCondition(1000, 100, new Condition() {
            @Override
            public boolean isTrue() {
                return scHandler.reads.size() >= 1;
            }
        }));
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
        assertTrue(waitForCondition(2000, 100, new Condition() {
            @Override
            public boolean isTrue() {
                return scHandler.reads.size() >= 2;
            }
        }));
        assertEquals(2, scHandler.reads.size());
        assertEquals(src.length, scHandler.reads.get(1).length);
        for (int i = 0; i < src.length; i++) {
            assertEquals(src[i], scHandler.reads.get(1)[i]);
        }
        handler.terminate();
    }

    public void testWrite() throws Exception {
        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setSend_buffer_size(1024);
        socketOptions.setReceive_buffer_size(1024);
        socketOptions.setTimeout(100);
        final SimpleCommHandlerFactory factory = new SimpleCommHandlerFactory();
        final ServerSocketChannelHandler handler = new ServerSocketChannelHandler(
                                                                                  "Test Handler",
                                                                                  new SocketOptions(),
                                                                                  new InetSocketAddress(
                                                                                                        "127.0.0.1",
                                                                                                        0),
                                                                                  Executors.newCachedThreadPool(),
                                                                                  factory);
        handler.start();
        InetSocketAddress endpont = handler.getLocalAddress();
        final SocketChannel inbound = SocketChannel.open();
        socketOptions.configure(inbound.socket());
        inbound.configureBlocking(false);
        inbound.connect(endpont);
        assertTrue(waitForCondition(4000, 100, new Condition() {
            @Override
            public boolean isTrue() {
                return factory.handlers.size() >= 1;
            }
        }));
        final SimpleCommHandler scHandler = factory.handlers.get(0);
        assertTrue(waitForCondition(4000, 100, new Condition() {
            @Override
            public boolean isTrue() {
                return scHandler.accepted.get();
            }
        }));

        assertTrue(inbound.finishConnect());

        final byte[][] src = new byte[2][];

        ByteBuffer buf = ByteBuffer.wrap(new byte[8192]);
        src[0] = new byte[8192];
        Arrays.fill(src[0], (byte) 6);
        buf.put(src[0]);
        buf.flip();
        scHandler.writes.add(buf);

        int testLength = 8192;
        buf = ByteBuffer.wrap(new byte[testLength]);
        src[1] = new byte[testLength];
        Arrays.fill(src[1], (byte) 12);
        buf.put(src[1]);
        buf.flip();
        scHandler.writes.add(buf);

        scHandler.selectForWrite();

        final ByteArrayOutputStream testBuf = new ByteArrayOutputStream();
        final ByteBuffer readBuf = ByteBuffer.wrap(new byte[512]);
        readBuf.clear();
        assertTrue(waitForCondition(4000, 100, new Condition() {
            @Override
            public boolean isTrue() {
                try {
                    int read = inbound.read(readBuf);
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
        }));
        byte[] testArray = testBuf.toByteArray();
        for (int i = 0; i < src[0].length; i++) {
            assertEquals(src[0][i], testArray[i]);
        }

        scHandler.selectForWrite();
        testBuf.reset();
        readBuf.clear();
        assertTrue(waitForCondition(4000, 100, new Condition() {
            @Override
            public boolean isTrue() {
                try {
                    int read = inbound.read(readBuf);
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
        }));
        testArray = testBuf.toByteArray();
        for (int i = 0; i < src[1].length; i++) {
            assertEquals(src[1][i], testArray[i]);
        }
    }
}
