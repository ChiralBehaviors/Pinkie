package com.hellblazer.pinkie.buffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import com.hellblazer.pinkie.ChannelHandler;
import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.CommunicationsHandlerFactory;

/**
 * A factory for constructing BufferProtocol instances so that they can be used
 * for accepts and connects
 * 
 * @author hhildebrand
 * 
 */
abstract public class BufferProtocolFactory implements
        CommunicationsHandlerFactory {

    public void connect(InetSocketAddress remoteAddress, ChannelHandler server)
                                                                               throws IOException {
        CommunicationsHandler handler = new BufferProtocol(
                                                           constructBufferProtocolHandler(remoteAddress)).getHandler();
        server.connectTo(remoteAddress, handler);
    }

    abstract public BufferProtocolHandler constructBufferProtocolHandler(InetSocketAddress remoteAddress);

    abstract public BufferProtocolHandler constructBufferProtocolHandler();

    @Override
    public CommunicationsHandler createCommunicationsHandler(SocketChannel channel) {
        return new BufferProtocol(constructBufferProtocolHandler()).getHandler();
    }
}
