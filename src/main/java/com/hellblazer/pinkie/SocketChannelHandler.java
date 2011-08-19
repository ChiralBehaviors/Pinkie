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
import java.util.logging.Level;
import java.util.logging.Logger;

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
            eventHandler.handleRead(channel);
        }
    }

    private class WriteHandler implements Runnable {
        @Override
        public void run() {
            eventHandler.handleWrite(channel);
        }
    }

    private static final Logger           log          = Logger.getLogger(SocketChannelHandler.class.getCanonicalName());

    final ReadHandler                     readHandler  = new ReadHandler();
    final WriteHandler                    writeHandler = new WriteHandler();
    private final SocketChannel           channel;
    private final ChannelHandler          handler;
    private volatile SocketChannelHandler next;
    private volatile boolean              open         = true;
    private volatile SocketChannelHandler previous;
    private final CommunicationsHandler   eventHandler;

    public SocketChannelHandler(CommunicationsHandler eventHandler,
                                ChannelHandler handler, SocketChannel channel) {
        this.eventHandler = eventHandler;
        this.handler = handler;
        this.channel = channel;
    }

    /**
     * Close the handler
     */
    public void close() {
        handler.closeHandler(this);
        if (log.isLoggable(Level.FINE)) {
            Exception e = new Exception("Socket close trace");
            log.log(Level.FINE, format("Closing connection to %s", channel), e);
        }
        internalClose();
    }

    /**
     * Answer true if the receiver is open
     * 
     * @return
     */
    public boolean open() {
        return open;
    }

    /**
     * Return the handler and select for read ready
     */
    public void selectForRead() {
        handler.selectForRead(this);
    }

    /**
     * Return the handler and select for read ready
     */
    public void selectForWrite() {
        handler.selectForWrite(this);
    }

    @Override
    public String toString() {
        Socket socket = channel.socket();
        return format("Handler for [local= %s, remote= %s]",
                      socket.getLocalSocketAddress(),
                      socket.getRemoteSocketAddress());
    }

    final Runnable acceptHandler() {
        return new Runnable() {
            @Override
            public void run() {
                eventHandler.handleAccept(channel, SocketChannelHandler.this);
            }
        };
    }

    final Runnable connectHandler() {
        return new Runnable() {
            @Override
            public void run() {
                eventHandler.handleConnect(channel, SocketChannelHandler.this);
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

    SocketChannel getChannel() {
        return channel;
    }

    final void internalClose() {
        eventHandler.closing(channel);
        open = false;
        try {
            channel.close();
        } catch (IOException e) {
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