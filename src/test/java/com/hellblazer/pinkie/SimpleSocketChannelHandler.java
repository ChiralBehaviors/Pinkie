package com.hellblazer.pinkie;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class SimpleSocketChannelHandler extends SocketChannelHandler {

    final List<byte[]>     reads     = new ArrayList<byte[]>();
    final List<ByteBuffer> writes    = new CopyOnWriteArrayList<ByteBuffer>();
    boolean                accepted  = false;
    boolean                connected = false;

    public SimpleSocketChannelHandler(ChannelHandler handler,
                                      SocketChannel channel) {
        super(handler, channel);
    }

    @Override
    public void handleAccept(SocketChannel channel) {
        accepted = true;
    }

    @Override
    public void handleConnect(SocketChannel channel) {
        connected = true;
    }

    @Override
    public void handleRead(SocketChannel channel) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ByteBuffer buffer = ByteBuffer.wrap(new byte[1024]);
            for (int read = channel.read(buffer); read != 0; read = channel.read(buffer)) {
                buffer.flip();
                byte[] b = new byte[read];
                buffer.get(b, 0, read);
                baos.write(b);
                buffer.flip();
            }
            reads.add(baos.toByteArray());
        } catch (Throwable e) {
            throw new IllegalStateException(e);
        }
        selectForRead();
    }

    @Override
    public void handleWrite(SocketChannel channel) {
        if (writes.size() == 0) {
            return;
        }
        try {
            ByteBuffer buffer = writes.get(0);
            channel.write(buffer);
            if (!buffer.hasRemaining()) {
                writes.remove(0);
            } else {
                selectForWrite();
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void selectForWrite() {
        super.selectForWrite();
    }

    public void selectForRead() {
        super.selectForRead();
    }
}
