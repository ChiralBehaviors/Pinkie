/*
 * Copyright (c) 2013 Hal Hildebrand, all rights reserved.
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
package com.hellblazer.pinkie.buffer;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.hellblazer.pinkie.ChannelHandler;
import com.hellblazer.pinkie.ServerSocketChannelHandler;
import com.hellblazer.pinkie.SocketOptions;
import com.hellblazer.pinkie.Utils;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestBufferProtocol {

    private ChannelHandler             clientHandler;
    private ServerSocketChannelHandler serverHandler;

    @After
    public void cleanUp() {
        if (clientHandler != null) {
            clientHandler.terminate();
        }
        if (serverHandler != null) {
            serverHandler.terminate();
        }
    }

    @Test
    public void testFullDuplex() throws IOException, InterruptedException {
        int bufferSize = 1024;
        int targetSize = 4 * 1024;

        byte[] clientMessage = new byte[targetSize];
        for (int i = 0; i < targetSize; i++) {
            clientMessage[i] = 'c';
        }

        byte[] serverMessage = new byte[targetSize];
        for (int i = 0; i < targetSize; i++) {
            serverMessage[i] = 's';
        }

        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setSend_buffer_size(bufferSize);
        socketOptions.setReceive_buffer_size(bufferSize);
        socketOptions.setTimeout(100);

        final AtomicBoolean clientConnect = new AtomicBoolean();
        final AtomicBoolean serverAccepted = new AtomicBoolean();

        final AtomicBoolean clientMessageReceived = new AtomicBoolean();
        final AtomicBoolean serverMessageReceived = new AtomicBoolean();

        BufferProtocolHandler client = mock(BufferProtocolHandler.class);
        signalConnect(clientConnect, client);
        signalMessageReceived(clientMessageReceived, client);
        ByteBuffer clientReadBuffer = ByteBuffer.allocate(targetSize);
        clientReadBuffer.rewind();
        when(client.newReadBuffer()).thenReturn(clientReadBuffer);
        when(client.newWriteBuffer()).thenReturn(ByteBuffer.wrap(clientMessage));
        constructClientHandler(socketOptions);
        BufferProtocolFactory clientProtocolFactory = new BufferProtocolFactory(
                                                                                client);

        BufferProtocolHandler server = mock(BufferProtocolHandler.class);
        signalAccept(serverAccepted, server);
        signalMessageReceived(serverMessageReceived, server);
        ByteBuffer serverReadBuffer = ByteBuffer.allocate(targetSize);
        serverReadBuffer.rewind();
        when(server.newReadBuffer()).thenReturn(serverReadBuffer);
        when(server.newWriteBuffer()).thenReturn(ByteBuffer.wrap(serverMessage));
        constructServerHandler(socketOptions, server);

        ArgumentCaptor<BufferProtocol> clientBufferProtocol = ArgumentCaptor.forClass(BufferProtocol.class);
        ArgumentCaptor<BufferProtocol> serverBufferProtocol = ArgumentCaptor.forClass(BufferProtocol.class);

        clientHandler.start();
        serverHandler.start();

        clientProtocolFactory.connect(serverHandler.getLocalAddress(),
                                      clientHandler);

        waitForConnect(clientConnect, serverAccepted);

        verify(client).connected(clientBufferProtocol.capture());
        verify(server).accepted(serverBufferProtocol.capture());

        clientBufferProtocol.getValue().selectForRead();
        serverBufferProtocol.getValue().selectForRead();

        clientBufferProtocol.getValue().selectForWrite();
        serverBufferProtocol.getValue().selectForWrite();

        waitForMessages(clientMessageReceived, serverMessageReceived);

        validate(serverMessage, clientBufferProtocol.getValue().getReadBuffer());
        validate(clientMessage, serverBufferProtocol.getValue().getReadBuffer());

        clientBufferProtocol.getValue().close();
        serverBufferProtocol.getValue().close();

        verify(client).closing();
        verify(server).closing();
    }

    private void constructClientHandler(SocketOptions socketOptions)
                                                                    throws IOException {
        clientHandler = new ChannelHandler("Client", socketOptions,
                                           Executors.newCachedThreadPool());
    }

    private void constructServerHandler(SocketOptions socketOptions,
                                        BufferProtocolHandler server)
                                                                     throws IOException {
        serverHandler = new ServerSocketChannelHandler(
                                                       "Server",
                                                       socketOptions,
                                                       new InetSocketAddress(
                                                                             "127.0.0.1",
                                                                             0),
                                                       Executors.newCachedThreadPool(),
                                                       new BufferProtocolFactory(
                                                                                 server));
    }

    private void signalAccept(final AtomicBoolean serverAccepted,
                              BufferProtocolHandler server) {
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                serverAccepted.set(true);
                return null;
            }
        }).when(server).accepted(any(BufferProtocol.class));
    }

    private void signalConnect(final AtomicBoolean clientConnect,
                               BufferProtocolHandler client) {
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                clientConnect.set(true);
                return null;
            }
        }).when(client).connected(any(BufferProtocol.class));
    }

    private void signalMessageReceived(final AtomicBoolean signal,
                                       BufferProtocolHandler handler) {
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                signal.set(true);
                return null;
            }
        }).when(handler).readReady();
    }

    private void validate(byte[] expected, ByteBuffer actual) {
        actual.flip();
        byte[] actualBytes = new byte[expected.length];
        actual.get(actualBytes);
        assertArrayEquals(expected, actualBytes);
    }

    private void waitForConnect(final AtomicBoolean clientConnect,
                                final AtomicBoolean serverAccepted)
                                                                   throws InterruptedException {
        Utils.waitFor("Client never connected", new Utils.Condition() {
            @Override
            public boolean value() {
                return clientConnect.get();
            }
        }, 4000, 100);

        Utils.waitFor("Server never accepted", new Utils.Condition() {
            @Override
            public boolean value() {
                return serverAccepted.get();
            }
        }, 4000, 100);
    }

    private void waitForMessages(final AtomicBoolean clientMessageSignal,
                                 final AtomicBoolean serverMessageSignal)
                                                                         throws InterruptedException {
        Utils.waitFor("Client never received message", new Utils.Condition() {
            @Override
            public boolean value() {
                return clientMessageSignal.get();
            }
        }, 5000, 100);

        Utils.waitFor("Server never received message", new Utils.Condition() {
            @Override
            public boolean value() {
                return serverMessageSignal.get();
            }
        }, 5000, 100);
    }
}
