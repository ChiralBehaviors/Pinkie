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
import java.util.concurrent.RejectedExecutionException;

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

    private final CommunicationsHandlerFactory eventHandlerFactory;
    private final ServerSocketChannel          server;
    private final Thread                       acceptThread;
    private final Selector                     acceptSelector;

    public ServerSocketChannelHandler(String handlerName,
                                      ServerSocketChannel channel,
                                      SocketOptions socketOptions,
                                      ExecutorService commsExec,
                                      CommunicationsHandlerFactory factory)
                                                                           throws IOException {
        this(handlerName, channel, socketOptions, commsExec, factory, 1);
    }

    public ServerSocketChannelHandler(String handlerName,
                                      ServerSocketChannel channel,
                                      SocketOptions socketOptions,
                                      ExecutorService commsExec,
                                      CommunicationsHandlerFactory factory,
                                      int selectorQueues) throws IOException {
        super(handlerName, socketOptions, commsExec, selectorQueues);
        if (factory == null) {
            throw new IllegalArgumentException(
                                               "Event handler factory cannot be null");
        }
        eventHandlerFactory = factory;
        server = channel;
        server.configureBlocking(false);

        acceptThread = new Thread(acceptSelectorTask(),
                                  String.format("Selector[%s (accept)]", name));
        acceptThread.setDaemon(true);
        acceptSelector = Selector.open();
    }

    public ServerSocketChannelHandler(String handlerName,
                                      SocketOptions socketOptions,
                                      InetSocketAddress endpointAddress,
                                      ExecutorService commsExec,
                                      CommunicationsHandlerFactory factory)
                                                                           throws IOException {
        this(handlerName, socketOptions, endpointAddress, commsExec, factory, 1);

    }

    public ServerSocketChannelHandler(String handlerName,
                                      SocketOptions socketOptions,
                                      InetSocketAddress endpointAddress,
                                      ExecutorService commsExec,
                                      CommunicationsHandlerFactory factory,
                                      int selectorQueues) throws IOException {
        this(handlerName, bind(socketOptions, endpointAddress), socketOptions,
             commsExec, factory, selectorQueues);
    }

    /**
     * Answer the local address of the endpoint
     * 
     * @return
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
                            log.trace(String.format("Channel closed [%s]", name),
                                      e);
                        }
                    } catch (IOException e) {
                        if (log.isTraceEnabled()) {
                            log.trace(String.format("Error when selecting for accept [%s]",
                                                    name), e);
                        }
                    } catch (CancelledKeyException e) {
                        if (log.isTraceEnabled()) {
                            log.trace(String.format("Error when selecting for accept [%s]",
                                                    name), e);
                        }
                    } catch (Throwable e) {
                        log.error(String.format("Runtime exception when selecting for accept [%s]",
                                                name), e);
                    }
                }
            }
        };
    }

    private void select() throws IOException {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Selecting for accept [%s]", name));
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
                    log.trace(format("Cancelled Key: %s [%s]", key, name), e);
                }
            }
        }
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

    void handleAccept(SelectionKey key) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Handling accept [%s]", name));
        }
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        final SocketChannel accepted = server.accept();
        options.configure(accepted.socket());
        accepted.configureBlocking(false);
        if (log.isTraceEnabled()) {
            log.trace(String.format("Connection accepted: %s [%s]", accepted,
                                    name));
        }

        CommunicationsHandler commHandler = eventHandlerFactory.createCommunicationsHandler(accepted);
        if (commHandler == null) {
            accepted.close();
            return;
        }
        final int index = nextQueueIndex();
        final SocketChannelHandler handler = new SocketChannelHandler(
                                                                      commHandler,
                                                                      this,
                                                                      accepted,
                                                                      index);
        addHandler(handler);
        try {
            executor.execute(handler.acceptHandler());
        } catch (RejectedExecutionException e) {
            if (log.isWarnEnabled()) {
                log.warn(String.format("too busy to execute accept handling [%s] of [%s]",
                                       name, handler.getChannel()));
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
        try {
            server.register(acceptSelector, SelectionKey.OP_ACCEPT);
            log.info(String.format("Server socket registered for accept [%s]",
                                   name));
        } catch (ClosedChannelException e) {
            log.error(String.format("Unable to register accept on %s [%s]",
                                    server, name), e);
        }
        acceptThread.start();
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
            log.trace(String.format("Cannot close: %s [%s]", server, name), e);
        }
        acceptSelector.wakeup();
        try {
            acceptSelector.close();
        } catch (IOException e) {
            log.info(String.format("Error closing selector [%s]", name), e);
        }
        super.terminateService();
    }
}
