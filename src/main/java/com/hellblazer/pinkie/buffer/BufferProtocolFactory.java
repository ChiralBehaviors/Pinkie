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
public class BufferProtocolFactory implements CommunicationsHandlerFactory {

    private final BufferProtocolHandler protocol;

    public BufferProtocolFactory(BufferProtocolHandler protocol) {
        this.protocol = protocol;
    }

    public void connect(InetSocketAddress remoteAddress, ChannelHandler server)
                                                                               throws IOException {
        CommunicationsHandler handler = constructHandler();
        server.connectTo(remoteAddress, handler);
    }

    @Override
    public CommunicationsHandler createCommunicationsHandler(SocketChannel channel) {
        return constructHandler();
    }

    private CommunicationsHandler constructHandler() {
        return new BufferProtocol(protocol).getHandler();
    }
}
