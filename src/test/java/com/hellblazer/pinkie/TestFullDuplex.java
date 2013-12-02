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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.KeyStore;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;

import org.junit.Test;

import com.hellblazer.utils.Condition;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestFullDuplex {
    private static class Handler implements CommunicationsHandler {
        final AtomicReference<SocketChannelHandler> handler      = new AtomicReference<>();
        final String                                label;
        final ByteBuffer                            read;
        final AtomicBoolean                         readFinished = new AtomicBoolean();
        final ByteBuffer                            write;

        public Handler(String label, int targetSize, byte[] output) {
            this.label = label;
            write = ByteBuffer.wrap(output);
            read = ByteBuffer.allocate(targetSize);
        }

        @Override
        public void accept(SocketChannelHandler handler) {
            assert handler != null;
            this.handler.set(handler);
        }

        @Override
        public void closing() {
        }

        @Override
        public void connect(SocketChannelHandler handler) {
            assert handler != null;
            this.handler.set(handler);
        }

        @Override
        public void readReady() {
            try {
                handler.get().getChannel().read(read);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            if (!read.hasRemaining()) {
                readFinished.set(true);
            } else {
                handler.get().selectForRead();
            }
        }

        public void select() {
            assert handler.get() != null;
            handler.get().selectForRead();
            handler.get().selectForWrite();
        }

        @Override
        public String toString() {
            return String.format("Handler[%s]", label);
        }

        @Override
        public void writeReady() {
            try {
                handler.get().getChannel().write(write);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            if (write.hasRemaining()) {
                handler.get().selectForWrite();
            }
        }
    }

    private static class HandlerFactory implements CommunicationsHandlerFactory {
        final AtomicReference<Handler> handler = new AtomicReference<>();
        final String                   label;
        final byte[]                   output;
        final int                      targetSize;

        public HandlerFactory(String label, int targetSize, byte[] output) {
            this.label = label;
            this.targetSize = targetSize;
            this.output = output;
        }

        @Override
        public CommunicationsHandler createCommunicationsHandler(SocketChannel channel) {
            handler.set(new Handler(label, targetSize, output));
            return handler.get();
        }

    }

    private static final String KEY_STORE   = "keystore.jks";

    private static char[]       PASS_PHRASE = "passphrase".toCharArray();

    private static final String TRUST_STORE = "cacerts.jks";

    private static SSLContext createSSLContext(boolean clientMode)
                                                                  throws Exception {
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(TestFullDuplex.class.getResourceAsStream(KEY_STORE),
                PASS_PHRASE);

        KeyStore ts = KeyStore.getInstance("JKS");
        ts.load(TestFullDuplex.class.getResourceAsStream(TRUST_STORE),
                PASS_PHRASE);
        SSLContext sslContext = SSLContext.getInstance("TLS");

        TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
        tmf.init(ts);
        sslContext.init(null, tmf.getTrustManagers(), null);

        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(ks, PASS_PHRASE);
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        return sslContext;
    }

    @Test
    public void testFullDuplex() throws Exception {

        SocketOptions socketOptions = new SocketOptions();
        int bufferSize = 1024;
        socketOptions.setSend_buffer_size(bufferSize);
        socketOptions.setReceive_buffer_size(bufferSize);
        socketOptions.setTimeout(100);

        int targetSize = 4 * 1024;

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
                                                                             Executors.newCachedThreadPool(),
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
                                                                             Executors.newCachedThreadPool(),
                                                                             factoryB);

        final Handler initiator = new Handler("A", targetSize, inputA);

        testFullDuplex(handlerA, handlerB, initiator, factoryB);
    }

    @Test
    public void testFullDuplexTls() throws Exception {
        SSLParameters sslParameters = new SSLParameters();
        SocketOptions socketOptions = new SocketOptions();
        int bufferSize = 1024;
        socketOptions.setSend_buffer_size(bufferSize);
        socketOptions.setReceive_buffer_size(bufferSize);
        socketOptions.setTimeout(100);

        int targetSize = 4 * 1024;

        byte[] inputA = new byte[targetSize];
        for (int i = 0; i < targetSize; i++) {
            inputA[i] = 'a';
        }
        HandlerFactory factoryA = new HandlerFactory("A", targetSize, null);
        ServerSocketChannelHandler handlerA = new ServerSocketChannelHandler(
                                                                             "A",
                                                                             socketOptions,
                                                                             new InetSocketAddress(
                                                                                                   "localhost",
                                                                                                   0),
                                                                             Executors.newCachedThreadPool(),
                                                                             factoryA,
                                                                             createSSLContext(false),
                                                                             sslParameters);

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
                                                                                                   "localhost",
                                                                                                   0),
                                                                             Executors.newCachedThreadPool(),
                                                                             factoryB,
                                                                             createSSLContext(false),
                                                                             sslParameters);

        final Handler initiator = new Handler("A", targetSize, inputA);

        testFullDuplex(handlerA, handlerB, initiator, factoryB);
    }

    private void testFullDuplex(ServerSocketChannelHandler handlerA,
                                ServerSocketChannelHandler handlerB,
                                final Handler initiator,
                                final HandlerFactory factoryB) throws Exception {

        handlerA.start();
        handlerB.start();

        handlerA.connectTo(handlerB.getLocalAddress(), initiator);

        waitForCondition(1000, 100, new Condition() {

            @Override
            public boolean isTrue() {
                return initiator.handler.get() != null;
            }
        });

        waitForCondition(1000, 100, new Condition() {
            @Override
            public boolean isTrue() {
                Handler handler = factoryB.handler.get();
                return handler != null && handler.handler.get() != null;
            }
        });

        final Handler acceptor = factoryB.handler.get();
        initiator.select();
        acceptor.select();

        waitForCondition(1000, 100, new Condition() {
            @Override
            public boolean isTrue() {
                return initiator.readFinished.get();
            }
        });

        waitForCondition(1000, 100, new Condition() {
            @Override
            public boolean isTrue() {
                return acceptor.readFinished.get();
            }
        });

        handlerA.terminate();
        handlerB.terminate();

    }
}
