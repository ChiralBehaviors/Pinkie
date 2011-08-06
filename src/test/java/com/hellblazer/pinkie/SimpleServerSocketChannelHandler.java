package com.hellblazer.pinkie;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;

public class SimpleServerSocketChannelHandler extends
        ServerSocketChannelHandler {

    public SimpleServerSocketChannelHandler(String handlerName,
                                            SelectableChannel channel,
                                            InetSocketAddress endpointAddress,
                                            SocketOptions socketOptions,
                                            ExecutorService commsExec)
                                                                      throws IOException {
        super(handlerName, channel, endpointAddress, socketOptions, commsExec);
    }

    @Override
    protected SocketChannelHandler createHandler(SocketChannel channel) {
        return new SimpleSocketChannelHandler(this, channel);
    }

}
