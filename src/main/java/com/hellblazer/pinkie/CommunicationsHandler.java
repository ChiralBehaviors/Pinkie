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

import java.nio.channels.SocketChannel;

/**
 * The interface that a communications handler implements to handle the
 * read/write/accept/connect events on a socket channel.
 * 
 * Implementors of handlers will be called back on one of the four methods:
 * 
 * <pre>
 *      handleConnect(SocketChannel)
 *      handleRead(SocketChannel)
 *      handleWrite(SocketChannel)
 *      handleAccept(SocketChannel)
 * </pre>
 * 
 * when the socket becomes connected, read ready, write ready or is accepted.
 * Note that connected is valid only for outbound connections, and accepted is
 * valid only for inbound connections.
 * 
 * After servicing the socket handler, the implementor must call either the
 * methods:
 * 
 * <pre>
 *      SocketChannelHandler.selectForRead()
 *      SocketChannelHandler.selectForWrite()
 * </pre>
 * 
 * to place the socket back in the select queue.
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public interface CommunicationsHandler {

    /**
     * Handle the accept of the socket
     * 
     * @param channel
     */
    void handleAccept(SocketChannel channel, SocketChannelHandler handler);

    /**
     * Handle the connection of the outbound socket
     * 
     * @param channel
     */
    void handleConnect(SocketChannel channel, SocketChannelHandler handler);

    /**
     * Handle the read ready socket
     * 
     * @param channel
     */
    void handleRead(SocketChannel channel, SocketChannelHandler handler);

    /**
     * Handle the write ready socket
     * 
     * @param channel
     */
    void handleWrite(SocketChannel channel, SocketChannelHandler handler);

    /**
     * The channel is closing, perform any clean up necessary
     */
    void closing(SocketChannel channel);
}
