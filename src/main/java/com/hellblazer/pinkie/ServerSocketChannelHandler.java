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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class ServerSocketChannelHandler<T extends CommunicationsHandler>
        extends ChannelHandler<T> {

    private static Logger log = Logger.getLogger(ServerSocketChannelHandler.class.getCanonicalName());

    public static ServerSocketChannel bind(SocketOptions options,
                                           InetSocketAddress endpointAddress)
                                                                             throws IOException {
        ServerSocketChannel server = ServerSocketChannel.open();
        ServerSocket serverSocket = server.socket();
        serverSocket.bind(endpointAddress, options.getBacklog());
        return server;
    }

    public static InetSocketAddress getLocalAddress(ServerSocketChannel channel) {

        return new InetSocketAddress(channel.socket().getInetAddress(),
                                     channel.socket().getLocalPort());
    }

    public ServerSocketChannelHandler(String handlerName,
                                      SelectableChannel channel,
                                      InetSocketAddress endpointAddress,
                                      SocketOptions socketOptions,
                                      Executor commsExec,
                                      CommunicationsHandlerFactory<T> factory)
                                                                              throws IOException {
        super(handlerName, channel, endpointAddress, socketOptions, commsExec,
              factory);
    }

    public ServerSocketChannelHandler(String handlerName,
                                      ServerSocketChannel channel,
                                      SocketOptions socketOptions,
                                      Executor commsExec,
                                      CommunicationsHandlerFactory<T> factory)
                                                                              throws IOException {
        this(handlerName, channel, getLocalAddress(channel), socketOptions,
             commsExec, factory);
    }

    public ServerSocketChannelHandler(String handlerName,
                                      SocketOptions socketOptions,
                                      InetSocketAddress endpointAddress,
                                      Executor commsExec,
                                      CommunicationsHandlerFactory<T> factory)
                                                                              throws IOException {
        this(handlerName, bind(socketOptions, endpointAddress), socketOptions,
             commsExec, factory);
    }

    /**
     * Connect to the remote address. The connection will be made in a
     * non-blocking fashion. The
     * CommunicationsHandler.handleConnect(SocketChannel) on the event handler
     * will be called when the socket channel actually connects.
     * 
     * @param remoteAddress
     * @param eventHandler
     * @throws IOException
     */
    public void connectTo(InetSocketAddress remoteAddress, T eventHandler)
                                                                          throws IOException {
        SocketChannel socketChannel = SocketChannel.open();
        options.configure(socketChannel.socket());
        SocketChannelHandler<T> handler = new SocketChannelHandler<T>(
                                                                      eventHandler,
                                                                      this,
                                                                      socketChannel);
        addHandler(handler);
        SelectionKey key = register(socketChannel, handler, 0);
        socketChannel.configureBlocking(false);
        if (socketChannel.connect(remoteAddress)) {
            try {
                commsExecutor.execute(handler.connectHandler());
            } catch (RejectedExecutionException e) {
                if (log.isLoggable(Level.INFO)) {
                    log.log(Level.INFO,
                            "too busy to execute connection handling");
                }
            }
            return;
        }
        key.interestOps(key.interestOps() | SelectionKey.OP_CONNECT);
        wakeup();
        return;
    }

    @Override
    void dispatch(SelectionKey key) throws IOException {
        if (key.isConnectable()) {
            handleConnect(key);
        } else {
            super.dispatch(key);
        }
    }

    void handleConnect(SelectionKey key) {
        if (log.isLoggable(Level.FINEST)) {
            log.finest("Handling read");
        }
        key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT);
        try {
            ((SocketChannel) key.channel()).finishConnect();
        } catch (IOException e) {
            log.log(Level.SEVERE, "Unable to finish connection", e);
        }
        if (log.isLoggable(Level.FINE)) {
            log.fine("Dispatching connected action");
        }
        try {
            @SuppressWarnings("unchecked")
            SocketChannelHandler<T> handler = (SocketChannelHandler<T>) key.attachment();
            commsExecutor.execute(handler.connectHandler());
        } catch (RejectedExecutionException e) {
            if (log.isLoggable(Level.FINEST)) {
                log.log(Level.FINEST, "cannot execute connect action", e);
            }
        }
    }
}
