package com.hellblazer.pinkie.buffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import com.hellblazer.pinkie.ChannelHandler;
import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.CommunicationsHandlerFactory;

/**
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

    private CommunicationsHandler constructHandler() {
        return new BufferProtocol(protocol.newReadBuffer(), protocol,
                                  protocol.newWriteBuffer()).getHandler();
    }

    @Override
    public CommunicationsHandler createCommunicationsHandler(SocketChannel channel) {
        return constructHandler();
    }
}
