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
import java.util.concurrent.atomic.AtomicBoolean;
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
            eventHandler.readReady();
        }
    }

    private class WriteHandler implements Runnable {
        @Override
        public void run() {
            eventHandler.writeReady();
        }
    }

    private static final Logger   log          = Logger.getLogger(SocketChannelHandler.class.getCanonicalName());

    private final SocketChannel   channel;
    private CommunicationsHandler eventHandler;
    private final ChannelHandler  handler;
    private SocketChannelHandler  next;
    private final AtomicBoolean   open         = new AtomicBoolean(true);
    private SocketChannelHandler  previous;
    private final Runnable        selectForRead;
    private final Runnable        selectForWrite;
    final ReadHandler             readHandler  = new ReadHandler();
    final WriteHandler            writeHandler = new WriteHandler();

    public SocketChannelHandler(CommunicationsHandler eventHandler,
                                ChannelHandler handler, SocketChannel channel) {
        this.eventHandler = eventHandler;
        this.handler = handler;
        this.channel = channel;
        selectForRead = handler.selectForRead(this);
        selectForWrite = handler.selectForWrite(this);
    }

    /**
     * Close the handler
     */
    public void close() {
        if (open.compareAndSet(true, false)) {
            if (log.isLoggable(Level.FINEST)) {
                Exception e = new Exception("Socket close trace");
                log.log(Level.FINEST,
                        format("Closing connection to %s", channel), e);
            }
            eventHandler.closing();
            try {
                channel.close();
            } catch (IOException e) {
                if (log.isLoggable(Level.FINEST)) {
                    log.log(Level.FINEST,
                            String.format("Error closing channel %s", channel),
                            e);
                }
            }
        }
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public CommunicationsHandler getHandler() {
        return eventHandler;
    }

    /**
     * Answer true if the receiver is open
     * 
     * @return
     */
    public boolean open() {
        return open.get();
    }

    /**
     * Reset the event handler
     * 
     * @param handler
     */
    public void resetHandler(CommunicationsHandler handler) {
        eventHandler = handler;
    }

    /**
     * Return the handler and select for read ready
     */
    public void selectForRead() {
        handler.register(selectForRead);
    }

    /**
     * Return the handler and select for read ready
     */
    public void selectForWrite() {
        handler.register(selectForWrite);
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
                eventHandler.accept(SocketChannelHandler.this);
            }
        };
    }

    final Runnable connectHandler() {
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