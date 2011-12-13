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

import static junit.framework.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestFullDuplex {
    private static interface Condition {
        boolean value();
    }

    private static class Handler implements CommunicationsHandler {
        volatile SocketChannelHandler handler;
        final String                  label;
        final ByteBuffer              read;
        final AtomicBoolean           readFinished = new AtomicBoolean();
        final ByteBuffer              write;

        public Handler(String label, int targetSize, byte[] output) {
            this.label = label;
            write = ByteBuffer.wrap(output);
            read = ByteBuffer.allocate(targetSize);
        }

        @Override
        public void accept(SocketChannelHandler handler) {
            this.handler = handler;
        }

        @Override
        public void closing() {
        }

        @Override
        public void connect(SocketChannelHandler handler) {
            this.handler = handler;
        }

        @Override
        public void readReady() {
            try {
                handler.getChannel().read(read);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            if (!read.hasRemaining()) {
                readFinished.set(true);
            } else {
                handler.selectForRead();
            }
        }

        public void select() {
            handler.selectForRead();
            handler.selectForWrite();
        }

        @Override
        public String toString() {
            return String.format("Handler[%s]", label);
        }

        @Override
        public void writeReady() {
            try {
                handler.getChannel().write(write);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            if (write.hasRemaining()) {
                handler.selectForWrite();
            }
        }
    }

    private static class HandlerFactory implements CommunicationsHandlerFactory {
        volatile Handler handler;
        final String     label;
        final byte[]     output;
        final int        targetSize;

        public HandlerFactory(String label, int targetSize, byte[] output) {
            this.label = label;
            this.targetSize = targetSize;
            this.output = output;
        }

        @Override
        public CommunicationsHandler createCommunicationsHandler(SocketChannel channel) {
            handler = new Handler(label, targetSize, output);
            return handler;
        }

    }

    @Test
    public void testFullDuplex() throws Exception {
        SocketOptions socketOptions = new SocketOptions();
        int bufferSize = 1024;
        socketOptions.setSend_buffer_size(bufferSize);
        socketOptions.setReceive_buffer_size(bufferSize);
        socketOptions.setTimeout(100);

        int targetSize = 4 * 1024 * 1024;

        System.out.println("Target: " + targetSize);

        byte[] inputA = new byte[targetSize];
        for (int i = 0; i < targetSize; i++) {
            inputA[i] = 'a';
        }
        HandlerFactory factoryA = new HandlerFactory("A", targetSize, null);
        ServerSocketChannelHandler handlerA = new ServerSocketChannelHandler(
                                                                             "A",
                                                                             socketOptions,
                                                                             new InetSocketAddress(
                                                                                                   "127.0.0.1",
                                                                                                   0),
                                                                             Executors.newFixedThreadPool(3),
                                                                             factoryA);

        byte[] inputB = new byte[targetSize];
        for (int i = 0; i < targetSize; i++) {
            inputB[i] = 'b';
        }
        final HandlerFactory factoryB = new HandlerFactory("B", targetSize,
                                                           inputB);
        ServerSocketChannelHandler handlerB = new ServerSocketChannelHandler(
                                                                             "B",
                                                                             socketOptions,
                                                                             new InetSocketAddress(
                                                                                                   "127.0.0.1",
                                                                                                   0),
                                                                             Executors.newFixedThreadPool(3),
                                                                             factoryB);

        final Handler initiator = new Handler("A", targetSize, inputA);

        handlerA.start();
        handlerB.start();

        handlerA.connectTo(handlerB.getLocalAddress(), initiator);

        waitFor("initiator not connected", new Condition() {
            @Override
            public boolean value() {
                return initiator.handler != null;
            }
        }, 10000, 100);

        waitFor("acceptor not connected", new Condition() {
            @Override
            public boolean value() {
                return factoryB.handler != null;
            }
        }, 10000, 100);

        final Handler acceptor = factoryB.handler;
        initiator.select();
        acceptor.select();

        waitFor("initiator failed to read fully", new Condition() {
            @Override
            public boolean value() {
                return initiator.readFinished.get();
            }
        }, 100000, 100);

        waitFor("acceptor failed to read fully", new Condition() {
            @Override
            public boolean value() {
                return acceptor.readFinished.get();
            }
        }, 100000, 100);

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
