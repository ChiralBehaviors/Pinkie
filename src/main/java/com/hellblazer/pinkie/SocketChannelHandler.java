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
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
abstract public class SocketChannelHandler {
    private static final Logger           log  = Logger.getLogger(SocketChannelHandler.class.getCanonicalName());

    private final ChannelHandler          handler;
    private final SocketChannel           channel;
    private volatile boolean              open = true;
    private volatile SocketChannelHandler next;
    private volatile SocketChannelHandler previous;

    public SocketChannelHandler(ChannelHandler handler, SocketChannel channel) {
        this.handler = handler;
        this.channel = channel;
    }

    public void close() {
        handler.closeHandler(this);
        if (log.isLoggable(Level.FINE)) {
            Exception e = new Exception("Socket close trace");
            log.log(Level.FINE, format("Closing connection to %s", channel), e);
        }
        internalClose();
    }

    public boolean connected() {
        return open;
    }

    abstract public void handleAccept(SocketChannel channel);

    abstract public void handleRead(SocketChannel channel);

    abstract public void handleWrite(SocketChannel channel);

    @Override
    public String toString() {
        Socket socket = channel.socket();
        return format("Handler for [local= %s, remote= %s]",
                      socket.getLocalSocketAddress(),
                      socket.getRemoteSocketAddress());
    }

    protected void closing() {
        // default is to do nothing
    }

    protected void delink() {
        if (previous != null) {
            previous.next = next;
        }
        if (next != null) {
            next.previous = previous;
        }
        next = previous = null;
    }

    protected SocketChannel getChannel() {
        return channel;
    }

    protected void handleAccept() {
        handleAccept(channel);
    }

    protected void handleRead() {
        handleRead(channel);
    }

    protected void handleWrite() {
        handleWrite(channel);
    }

    protected final void internalClose() {
        closing();
        open = false;
        try {
            channel.close();
        } catch (IOException e) {
        }
    }

    protected void link(SocketChannelHandler h) {
        h.previous = this;
        next = h;
    }

    protected SocketChannelHandler next() {
        return next;
    }

    protected void selectForRead() {
        handler.selectForRead(this);
    }

    protected void selectForWrite() {
        handler.selectForWrite(this);
    }
}