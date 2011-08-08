package com.hellblazer.pinkie;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

public class SimpleServerSocketChannelHandler extends
        ServerSocketChannelHandler {
    List<SimpleSocketChannelHandler> handlers = new ArrayList<SimpleSocketChannelHandler>();

    public SimpleServerSocketChannelHandler(String handlerName,
                                            SocketOptions socketOptions,
                                            InetSocketAddress endpointAddress,
                                            Executor commsExec)
                                                               throws IOException {
        super(handlerName, socketOptions, endpointAddress, commsExec);
    }

    @Override
    protected SocketChannelHandler createHandler(SocketChannel channel) {
        SimpleSocketChannelHandler handler = new SimpleSocketChannelHandler(
                                                                            this,
                                                                            channel);
        handlers.add(handler);
        return handler;
    }
}
