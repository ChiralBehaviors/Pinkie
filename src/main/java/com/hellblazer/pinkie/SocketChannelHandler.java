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
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The handler for socket channels. This class provides the bridge to interact
 * with event handler for the server socket channel handler to provide non
 * blocking read, writes and accepts.
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class SocketChannelHandler {

    private class ReadHandler implements Runnable {
        @Override
        public void run() {
            eventHandler.readReady();
        }
    }

    private class WriteHandler implements Runnable {
        @Override
        public void run() {
            eventHandler.writeReady();
        }
    }

    private static final Logger           log          = LoggerFactory.getLogger(SocketChannelHandler.class);

    private final CommunicationsHandler   eventHandler;
    private final int                     index;
    private volatile SocketChannelHandler next;
    private volatile SocketChannelHandler previous;
    private final ReadHandler             readHandler  = new ReadHandler();
    private final Runnable                selectForRead;
    private final Runnable                selectForWrite;
    private final WriteHandler            writeHandler = new WriteHandler();

    final SocketChannel                   channel;
    final ChannelHandler                  handler;
    final AtomicBoolean                   open         = new AtomicBoolean(true);

    public SocketChannelHandler(CommunicationsHandler eventHandler,
                                ChannelHandler handler, SocketChannel channel,
                                int index) {
        this.eventHandler = eventHandler;
        this.handler = handler;
        this.channel = channel;
        this.index = index;
        selectForRead = handler.selectForRead(index, this);
        selectForWrite = handler.selectForWrite(index, this);
    }

    /**
     * Close the handler
     */
    public void close() {
        if (open.compareAndSet(true, false)) {
            if (log.isTraceEnabled()) {
                Exception e = new Exception("Socket close trace");
                log.trace(format("Closing connection to %s", channel), e);
            }
            try {
                channel.close();
            } catch (IOException e) {
                if (log.isTraceEnabled()) {
                    log.trace(String.format("Error closing channel %s", channel),
                              e);
                }
            }
            handler.wakeup(index);
            handler.execute(new Runnable() {
                @Override
                public void run() {
                    eventHandler.closing();
                }
            });
        }
    }

    /**
     * 
     * @return the underlying SocketChannel for this handler
     */
    public SocketChannel getChannel() {
        return channel;
    }

    /**
     * 
     * @return the SSL session for the channel, null if not TLS
     */
    public SSLSession getSslSession() {
        return null;
    }

    /**
     * 
     * @return true if the receiver is open
     */
    public boolean open() {
        return open.get();
    }

    /**
     * Select for read ready
     */
    public void selectForRead() {
        handler.register(index, selectForRead);
    }

    /**
     * Select for write ready
     */
    public void selectForWrite() {
        handler.register(index, selectForWrite);
    }

    @Override
    public String toString() {
        Socket socket = channel.socket();
        return format("Handler for [local= %s, remote= %s]",
                      socket.getLocalSocketAddress(),
                      socket.getRemoteSocketAddress());
    }

    private Runnable acceptHandler() {
        return new Runnable() {
            @Override
            public void run() {
                eventHandler.accept(SocketChannelHandler.this);
            }
        };
    }

    private Runnable connectHandler() {
        return new Runnable() {
            @Override
            public void run() {
                eventHandler.connect(SocketChannelHandler.this);
            }
        };
    }

    /**
     * Private protocol to maintainthe linked list of handlers
     */
    final void delink() {
        if (previous != null) {
            previous.next = next;
        }
        if (next != null) {
            next.previous = previous;
        }
        next = previous = null;
    }

    CommunicationsHandler getHandler() {
        return eventHandler;
    }

    void handleAccept() {
        try {
            handler.execute(acceptHandler());
        } catch (RejectedExecutionException e) {
            if (log.isWarnEnabled()) {
                log.warn(String.format("too busy to execute accept handling [%s] of [%s]",
                                       handler.getName(), channel));
            }
            close();
        }
    }

    void handleConnect() {
        try {
            handler.execute(connectHandler());
        } catch (RejectedExecutionException e) {
            if (log.isInfoEnabled()) {
                log.info(String.format("too busy to execute connect handling [%s] of [%s]",
                                       handler.getName(), channel));
            }
            close();
        }
    }

    void handleRead() {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Handling read [%s]", handler.getName()));
        }
        try {
            handler.execute(readHandler);
        } catch (RejectedExecutionException e) {
            if (log.isInfoEnabled()) {
                log.info(String.format("too busy to execute read handling [%s], reselecting [%s]",
                                       handler.getName(), channel));
            }
            selectForRead();
        }
    }

    void handleWrite() {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Handling write [%s]", handler.getName()));
        }
        try {
            handler.execute(writeHandler);
        } catch (RejectedExecutionException e) {
            if (log.isInfoEnabled()) {
                log.info(String.format("too busy to execute write handling [%s], reselecting [%s]",
                                       handler.getName(), channel));
            }
            selectForWrite();
        }
    }

    /**
     * Private protocol to maintainthe linked list of handlers
     */
    final void link(SocketChannelHandler h) {
        h.previous = this;
        next = h;
    }

    /**
     * Private protocol to maintainthe linked list of handlers
     */
    SocketChannelHandler next() {
        return next;
    }
}