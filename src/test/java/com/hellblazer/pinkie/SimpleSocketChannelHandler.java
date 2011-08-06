package com.hellblazer.pinkie;

import java.nio.channels.SocketChannel;

public class SimpleSocketChannelHandler extends SocketChannelHandler {

    public SimpleSocketChannelHandler(ChannelHandler handler,
                                      SocketChannel channel) {
        super(handler, channel);
        // TODO Auto-generated constructor stub
    }

    @Override
    public void handleAccept(SocketChannel channel) {
        // TODO Auto-generated method stub

    }

    @Override
    public void handleConnect(SocketChannel channel) {
        // TODO Auto-generated method stub

    }

    @Override
    public void handleRead(SocketChannel channel) {
        // TODO Auto-generated method stub

    }

    @Override
    public void handleWrite(SocketChannel channel) {
        // TODO Auto-generated method stub

    }

}
