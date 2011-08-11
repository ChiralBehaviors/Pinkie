/** (C) Copyright 2011 Hal Hildebrand, all rights reserved.

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
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
            eventHandler.handleRead(channel, SocketChannelHandler.this);
        }
    }

    private class WriteHandler implements Runnable {
        @Override
        public void run() {
            eventHandler.handleWrite(channel, SocketChannelHandler.this);
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