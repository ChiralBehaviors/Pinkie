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
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class ServerSocketChannelHandler extends ChannelHandler {

    private static Logger log = LoggerFactory.getLogger(ServerSocketChannelHandler.class);

    public static ServerSocketChannel bind(SocketOptions options,
                                           InetSocketAddress endpointAddress)
                                                                             throws IOException {
        ServerSocketChannel server = ServerSocketChannel.open();
        ServerSocket serverSocket = server.socket();
        options.configure(serverSocket);
        serverSocket.bind(endpointAddress, options.getBacklog());
        return server;
    }

    private final Selector                     acceptSelector;
    private final Thread                       acceptThread;
    private final CommunicationsHandlerFactory eventHandlerFactory;
    private final ServerSocketChannel          server;

    /**
     * Construct a new server socket channel handler with one selection queue
     * 
     * @param handlerName
     *            - the String name used to mark the selection thread
     * @param channel
     *            - the server socket channel
     * @param socketOptions
     *            - the socket options to configure new sockets
     * @param commsExec
     *            - the executor service to handle I/O and selection events
     * @param factory
     *            - the communications handler factory
     * @throws IOException
     *             - if things go pear shaped when opening the selector
     */
    public ServerSocketChannelHandler(String handlerName,
                                      ServerSocketChannel channel,
                                      SocketOptions socketOptions,
                                      ExecutorService commsExec,
                                      CommunicationsHandlerFactory factory)
                                                                           throws IOException {
        this(handlerName, channel, socketOptions, commsExec, factory, 1, null,
             null, DEFAULT_CHANNEL_FACTORY);
    }

    /**
     * Construct a new server socket channel handler
     * 
     * @param handlerName
     *            - the String name used to mark the selection thread
     * @param channel
     *            - the server socket channel
     * @param socketOptions
     *            - the socket options to configure new sockets
     * @param commsExec
     *            - the executor service to handle I/O and selection events
     * @param factory
     *            - the communications handler factory
     * @param selectorQueues
     *            - the number of selectors to use
     * @throws IOException
     *             - if things go pear shaped when opening the selector
     */
    public ServerSocketChannelHandler(String handlerName,
                                      ServerSocketChannel channel,
                                      SocketOptions socketOptions,
                                      ExecutorService commsExec,
                                      CommunicationsHandlerFactory factory,
                                      int selectorQueues) throws IOException {
        this(handlerName, channel, socketOptions, commsExec, factory,
             selectorQueues, null, null, DEFAULT_CHANNEL_FACTORY);
    }

    /**
     * Construct a new server socket channel handler
     * 
     * @param handlerName
     *            - the String name used to mark the selection thread
     * @param channel
     *            - the server socket channel
     * @param socketOptions
     *            - the socket options to configure new sockets
     * @param commsExec
     *            - the executor service to handle I/O and selection events
     * @param factory
     *            - the communications handler factory
     * @param selectorQueues
     *            - the number of selectors to use
     * @param sslContext
     *            - the sslContext to use for SSL sessions
     * @param sslParameters
     *            - SSL parameters to use
     * @param cFactory
     * 			  - The Factory to use for new Selectors & Channels
     * @throws IOException
     *             - if things go pear shaped when opening the selector
     */
    public ServerSocketChannelHandler(String handlerName,
                                      ServerSocketChannel channel,
                                      SocketOptions socketOptions,
                                      ExecutorService commsExec,
                                      CommunicationsHandlerFactory factory,
                                      int selectorQueues,
                                      SSLContext sslContext,
                                      SSLParameters sslParameters,
                                      ChannelFactory cFactory)
                                                                  throws IOException {
        super(handlerName, socketOptions, commsExec, selectorQueues, cFactory);
        if (factory == null) {
            throw new IllegalArgumentException(
                                               "Event handler factory cannot be null");
        }
        eventHandlerFactory = factory;
        server = channel;
        server.configureBlocking(false);

        acceptThread = new Thread(acceptSelectorTask(),
                                  String.format("Selector[%s (accept)]",
                                                getName()));
        acceptThread.setDaemon(true);
        acceptSelector = channelFactory.selectorOpen();
    }

    /**
     * Construct a new server socket channel handler with one selection queue
     * 
     * @param handlerName
     *            - the String name used to mark the selection thread
     * @param channel
     *            - the server socket channel
     * @param socketOptions
     *            - the socket options to configure new sockets
     * @param commsExec
     *            - the executor service to handle I/O and selection events
     * @param factory
     *            - the communications handler factory
     * @param sslContext
     *            - the sslContext to use for SSL sessions
     * @param sslParameters
     *            - SSL parameters to use
     * @throws IOException
     *             - if things go pear shaped when opening the selector
     */
    public ServerSocketChannelHandler(String handlerName,
                                      ServerSocketChannel channel,
                                      SocketOptions socketOptions,
                                      ExecutorService commsExec,
                                      CommunicationsHandlerFactory factory,
                                      SSLContext sslContext,
                                      SSLParameters sslParameters)
                                                                  throws IOException {
        this(handlerName, channel, socketOptions, commsExec, factory, 1,
             sslContext, sslParameters, DEFAULT_CHANNEL_FACTORY);
    }

    /**
     * Construct a new server socket channel handler with one selection queue
     * 
     * @param handlerName
     *            - the String name used to mark the selection thread
     * @param socketOptions
     *            - the socket options to configure new sockets
     * @param endpointAddress
     *            - the internet address and port to use for the server socket
     *            channel
     * @param commsExec
     *            - the executor service to handle I/O and selection events
     * @param factory
     *            - the communications handler factory
     * @throws IOException
     *             - if things go pear shaped when opening the selector or
     *             binding the server socket to the address
     */
    public ServerSocketChannelHandler(String handlerName,
                                      SocketOptions socketOptions,
                                      InetSocketAddress endpointAddress,
                                      ExecutorService commsExec,
                                      CommunicationsHandlerFactory factory)
                                                                           throws IOException {
        this(handlerName, socketOptions, endpointAddress, commsExec, factory,
             1, null, null);
    }

    /**
     * Construct a new server socket channel handler
     * 
     * @param handlerName
     *            - the String name used to mark the selection thread
     * @param socketOptions
     *            - the socket options to configure new sockets
     * @param endpointAddress
     *            - the internet address and port to use for the server socket
     *            channel
     * @param commsExec
     *            - the executor service to handle I/O and selection events
     * @param factory
     *            - the communications handler factory
     * @param selectorQueues
     *            - the number of selectors to use
     * @param factory
     *            - the communications handler factory
     * @param sslContext
     *            - the sslContext to use for SSL sessions
     * @param sslParameters
     *            - SSL parameters to use
     * @throws IOException
     *             - if things go pear shaped when opening the selector or
     *             binding the server socket to the address
     */
    public ServerSocketChannelHandler(String handlerName,
                                      SocketOptions socketOptions,
                                      InetSocketAddress endpointAddress,
                                      ExecutorService commsExec,
                                      CommunicationsHandlerFactory factory,
                                      int selectorQueues,
                                      SSLContext sslContext,
                                      SSLParameters sslParameters)
                                                                  throws IOException {
        this(handlerName, bind(socketOptions, endpointAddress), socketOptions,
             commsExec, factory, selectorQueues, sslContext, sslParameters,
             DEFAULT_CHANNEL_FACTORY);
    }

    /**
     * Construct a new server socket channel handler with one selection queue
     * 
     * @param handlerName
     *            - the String name used to mark the selection thread
     * @param socketOptions
     *            - the socket options to configure new sockets
     * @param endpointAddress
     *            - the internet address and port to use for the server socket
     *            channel
     * @param commsExec
     *            - the executor service to handle I/O and selection events
     * @param factory
     *            - the communications handler factory
     * @param sslContext
     *            - the sslContext to use for SSL sessions
     * @param sslParameters
     *            - SSL parameters to use
     * @throws IOException
     *             - if things go pear shaped when opening the selector or
     *             binding the server socket to the address
     */
    public ServerSocketChannelHandler(String handlerName,
                                      SocketOptions socketOptions,
                                      InetSocketAddress endpointAddress,
                                      ExecutorService commsExec,
                                      CommunicationsHandlerFactory factory,
                                      SSLContext sslContext,
                                      SSLParameters sslParameters)
                                                                  throws IOException {
        this(handlerName, socketOptions, endpointAddress, commsExec, factory,
             1, sslContext, sslParameters);
    }

    /**
     * @return the local address of the endpoint
     */
    public InetSocketAddress getLocalAddress() {

        return (InetSocketAddress) server.socket().getLocalSocketAddress();
    }

    private Runnable acceptSelectorTask() {
        return new Runnable() {
            @Override
            public void run() {
                while (run.get()) {
                    try {
                        select();
                    } catch (ClosedSelectorException e) {
                        if (log.isTraceEnabled()) {
                            log.trace(String.format("Channel closed [%s]",
                                                    getName()), e);
                        }
                    } catch (IOException e) {
                        if (log.isTraceEnabled()) {
                            log.trace(String.format("Error when selecting for accept [%s]",
                                                    getName()), e);
                        }
                    } catch (CancelledKeyException e) {
                        if (log.isTraceEnabled()) {
                            log.trace(String.format("Error when selecting for accept [%s]",
                                                    getName()), e);
                        }
                    } catch (Throwable e) {
                        log.error(String.format("Runtime exception when selecting for accept [%s]",
                                                getName()), e);
                    }
                }
            }
        };
    }

    private void dispatch(SelectionKey key) throws IOException {
        if (key.isAcceptable()) {
            handleAccept(key);
        } else {
            throw new IllegalStateException(
                                            String.format("Invalid selection key for accept: %s",
                                                          key));
        }
    }

    private void select() throws IOException {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Selecting for accept [%s]", getName()));
        }

        acceptSelector.select(selectTimeout);

        // get an iterator over the set of selected keys
        Iterator<SelectionKey> selected;
        try {
            selected = acceptSelector.selectedKeys().iterator();
        } catch (ClosedSelectorException e) {
            return;
        }

        while (run.get() && selected.hasNext()) {
            SelectionKey key = selected.next();
            selected.remove();
            try {
                dispatch(key);
            } catch (CancelledKeyException e) {
                if (log.isTraceEnabled()) {
                    log.trace(format("Cancelled Key: %s [%s]", key, getName()),
                              e);
                }
            }
        }
    }

    void handleAccept(SelectionKey key) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Handling accept [%s]", getName()));
        }
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        final SocketChannel accepted = server.accept();
        options.configure(accepted.socket());
        accepted.configureBlocking(false);
        if (log.isTraceEnabled()) {
            log.trace(String.format("Connection accepted: %s [%s]", accepted,
                                    getName()));
        }

        CommunicationsHandler commHandler = eventHandlerFactory.createCommunicationsHandler(accepted);
        if (commHandler == null) {
            accepted.close();
            return;
        }
        SocketChannelHandler handler = new SocketChannelHandler(
                                                                commHandler,
                                                                this,
                                                                accepted,
                                                                nextQueueIndex());
        addHandler(handler);
        handler.handleAccept();
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.SelectableChannelHandler#startService()
     */
    @Override
    void startService() {
        super.startService();
        try {
            server.register(acceptSelector, SelectionKey.OP_ACCEPT);
            log.info(String.format("server socket registered for accept [%s]",
                                   getName()));
        } catch (ClosedChannelException e) {
            log.error(String.format("Unable to register accept on %s [%s]",
                                    server, getName()), e);
        }
        acceptThread.start();
        log.info(format("local address is %s for [%s]", getName(),
                        getLocalAddress(), getName()));
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.SelectableChannelHandler#terminateService()
     */
    @Override
    void terminateService() {
        try {
            server.close();
        } catch (IOException e) {
            log.trace(String.format("Cannot close: %s [%s]", server, getName()),
                      e);
        }
        acceptSelector.wakeup();
        try {
            acceptSelector.close();
        } catch (IOException e) {
            log.info(String.format("Error closing selector [%s]", getName()), e);
        }
        super.terminateService();
    }
}
