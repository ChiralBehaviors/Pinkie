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

/**
 * The interface that a communications handler implements to handle the
 * read/write/accept/connect events on a socket channel.
 * 
 * Implementors of handlers will be called back on one of the four methods:
 * 
 * <pre>
 *      accept(SocketChannelHandler)
 *      connect(SocketChannelHandler)
 *      readReady()
 *      writeReady()
 * </pre>
 * 
 * when the socket becomes connected, read ready, write ready or is accepted.
 * Note that connected is valid only for outbound connections, and accepted is
 * valid only for inbound connections.
 * 
 * The connected and accept methods include the SocketChannelHandler that is
 * responsible for the CommunicationsHandler. The CommunicationsHandler instance
 * is expected to store the SocketChannelHandler for future use.
 * 
 * After servicing the event, or at any time appropriate, the implementor must
 * call either of the methods:
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
     * Handle the accept of the socket. The SocketChannel
     * 
     * @param handler
     */
    void accept(SocketChannelHandler handler);

    /**
     * The channel is closing, perform any clean up necessary.
     * @param reason, if not null, the reason why the channel is closing
     */
    void closing(IOException reason);

    /**
     * Handle the connection of the outbound socket
     * 
     * @param handler
     */
    void connect(SocketChannelHandler handler);

    /**
     * Handle the read ready socket
     * 
     * @param channel
     */
    void readReady();

    /**
     * Handle the write ready socket
     * 
     * @param channel
     */
    void writeReady();
}
