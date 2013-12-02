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

package com.hellblazer.pinkie;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A wrapper around a SocketChannel to provide seamless Tls functionality for
 * the socket channel
 * 
 * @author hhildebrand
 * 
 */
public class TlsSocketChannel extends SocketChannel {
    private AtomicReference<Exception>    deferredException = new AtomicReference<>();
    private final TlsSocketChannelHandler handler;

    public TlsSocketChannel(TlsSocketChannelHandler handler) {
        super(handler.getConcreteChannel().provider());
        this.handler = handler;
    }

    @Override
    public SocketChannel bind(SocketAddress local) throws IOException {
        return handler.getConcreteChannel().bind(local);
    }

    @Override
    public boolean connect(SocketAddress remote) throws IOException {
        return handler.getConcreteChannel().connect(remote);
    }

    @Override
    public boolean equals(Object obj) {
        return handler.getConcreteChannel().equals(obj);
    }

    @Override
    public boolean finishConnect() throws IOException {
        return handler.getConcreteChannel().finishConnect();
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        return handler.getConcreteChannel().getLocalAddress();
    }

    @Override
    public <T> T getOption(SocketOption<T> name) throws IOException {
        return handler.getConcreteChannel().getOption(name);
    }

    @Override
    public SocketAddress getRemoteAddress() throws IOException {
        return handler.getConcreteChannel().getRemoteAddress();
    }

    @Override
    public int hashCode() {
        return handler.getConcreteChannel().hashCode();
    }

    @Override
    public boolean isConnected() {
        return handler.getConcreteChannel().isConnected();
    }

    @Override
    public boolean isConnectionPending() {
        return handler.getConcreteChannel().isConnectionPending();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return handler.read(dst);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length)
                                                               throws IOException {
        return handler.read(dsts, offset, length);
    }

    @Override
    public <T> SocketChannel setOption(SocketOption<T> name, T value)
                                                                     throws IOException {
        return handler.getConcreteChannel().setOption(name, value);
    }

    @Override
    public SocketChannel shutdownInput() throws IOException {
        return handler.getConcreteChannel().shutdownInput();
    }

    @Override
    public SocketChannel shutdownOutput() throws IOException {
        return handler.getConcreteChannel().shutdownOutput();
    }

    @Override
    public Socket socket() {
        return handler.getConcreteChannel().socket();
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        return handler.getConcreteChannel().supportedOptions();
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return handler.write(src);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length)
                                                                throws IOException {
        return handler.write(srcs, offset, length);
    }

    /* (non-Javadoc)
     * @see java.nio.channels.spi.AbstractSelectableChannel#implCloseSelectableChannel()
     */
    @Override
    protected void implCloseSelectableChannel() throws IOException {
        handler.getConcreteChannel().close();
    }

    /* (non-Javadoc)
     * @see java.nio.channels.spi.AbstractSelectableChannel#implConfigureBlocking(boolean)
     */
    @Override
    protected void implConfigureBlocking(boolean block) throws IOException {
        handler.getConcreteChannel().configureBlocking(block);
    }

    void setDeferredException(Exception deferredException) {
        this.deferredException.set(deferredException);
    }
}
