/*
 * Copyright (c) 2013 Hal Hildebrand, all rights reserved.
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
package com.hellblazer.pinkie.buffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.hellblazer.pinkie.ChannelHandler;
import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.CommunicationsHandlerFactory;

/**
 * A handler factory that can create per connection handler instances.
 * 
 * @author hhildebrand
 * 
 */
public class ParameterizedBufferProtocolFactory implements
        CommunicationsHandlerFactory {

    public static interface BufferProtocolHandlerProvider {
        BufferProtocolHandler newProtocolHandler();
    }

    private final BufferProtocolHandlerProvider provider;
    private final int                           readBufferSize;
    private final int                           writeBufferSize;
    private final boolean                       directBuffer;

    public ParameterizedBufferProtocolFactory(int readBufferSize,
                                              BufferProtocolHandlerProvider provider,
                                              int writeBufferSize,
                                              boolean directBuffer) {
        this.readBufferSize = readBufferSize;
        this.writeBufferSize = writeBufferSize;
        this.provider = provider;
        this.directBuffer = directBuffer;
    }

    private ByteBuffer allocateBuffer(int bufferSize) {
        return directBuffer ? ByteBuffer.allocateDirect(bufferSize)
                           : ByteBuffer.allocate(bufferSize);
    }

    public void connect(InetSocketAddress remoteAddress, ChannelHandler server)
                                                                               throws IOException {
        CommunicationsHandler handler = constructHandler();
        server.connectTo(remoteAddress, handler);
    }

    private CommunicationsHandler constructHandler() {
        BufferProtocolHandler protocol = provider.newProtocolHandler();
        return new BufferProtocol(allocateBuffer(readBufferSize), protocol,
                                  allocateBuffer(writeBufferSize)).getHandler();
    }

    @Override
    public CommunicationsHandler createCommunicationsHandler(SocketChannel channel) {
        return constructHandler();
    }

}
