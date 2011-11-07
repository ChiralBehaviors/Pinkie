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

import static java.lang.String.format;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
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
public class ServerSocketChannelHandler extends ChannelHandler {

    private static Logger log = Logger.getLogger(ServerSocketChannelHandler.class.getCanonicalName());

    public static ServerSocketChannel bind(SocketOptions options,
                                           InetSocketAddress endpointAddress)
                                                                             throws IOException {
        ServerSocketChannel server = ServerSocketChannel.open();
        ServerSocket serverSocket = server.socket();
        options.configure(serverSocket);
        serverSocket.bind(endpointAddress, options.getBacklog());
        return server;
    }

    private final CommunicationsHandlerFactory eventHandlerFactory;
    private final ServerSocketChannel          server;

    public ServerSocketChannelHandler(String handlerName,
                                      ServerSocketChannel channel,
                                      SocketOptions socketOptions,
                                      Executor commsExec,
                                      CommunicationsHandlerFactory factory)
                                                                           throws IOException {
        super(handlerName, socketOptions, commsExec);
        eventHandlerFactory = factory;
        server = channel;
        server.configureBlocking(false);
        server.register(selector, SelectionKey.OP_ACCEPT);
    }

    public ServerSocketChannelHandler(String handlerName,
                                      SocketOptions socketOptions,
                                      InetSocketAddress endpointAddress,
                                      Executor commsExec,
                                      CommunicationsHandlerFactory factory)
                                                                           throws IOException {
        this(handlerName, bind(socketOptions, endpointAddress), socketOptions,
             commsExec, factory);
    }

    /**
     * Answer the local address of the endpoint
     * 
     * @return
     */
    public InetSocketAddress getLocalAddress() {

        return (InetSocketAddress) server.socket().getLocalSocketAddress();
    }

    /**
     * Create an instance of a socket channel handler, using the supplied
     * channel and key.
     * 
     * @param channel
     * @return
     */
    SocketChannelHandler createHandler(SocketChannel channel) {
        return new SocketChannelHandler(
                                        eventHandlerFactory.createCommunicationsHandler(channel),
                                        this, channel);
    }

    @Override
    void dispatch(SelectionKey key) throws IOException {
        if (key.isAcceptable()) {
            handleAccept(key);
        } else {
            super.dispatch(key);
        }
    }

    void handleAccept(SelectionKey key) throws IOException {
        if (log.isLoggable(Level.FINEST)) {
            log.finest("Handling accept");
        }
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel accepted = server.accept();
        options.configure(accepted.socket());
        accepted.configureBlocking(false);
        if (log.isLoggable(Level.FINE)) {
            log.fine(String.format("Connection accepted: %s", accepted));
        }
        SocketChannelHandler handler = createHandler(accepted);
        SelectionKey newKey = register(accepted, handler, 0);
        addHandler(handler);
        newKey.attach(handler);
        try {
            commsExecutor.execute(handler.acceptHandler());
        } catch (RejectedExecutionException e) {
            if (log.isLoggable(Level.INFO)) {
                log.log(Level.INFO, "too busy to execute accept handling");
            }
            handler.close();
        }
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.SelectableChannelHandler#startService()
     */
    @Override
    void startService() {
        super.startService();
        log.info(format("%s local address: %s", name, getLocalAddress()));
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.SelectableChannelHandler#terminateService()
     */
    @Override
    void terminateService() {
        try {
            server.close();
        } catch (IOException e) {
            log.log(Level.FINEST, String.format("Cannot close: %s", server), e);
        }
        super.terminateService();
    }
}
