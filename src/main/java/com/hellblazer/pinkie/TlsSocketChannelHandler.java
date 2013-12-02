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
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A socket channel handler that wraps and unwraps the data with TLS
 * 
 * @author hhildebrand
 * 
 */
public class TlsSocketChannelHandler extends SocketChannelHandler {
    private static final Logger    log          = LoggerFactory.getLogger(TlsSocketChannelHandler.class);
    private final SSLEngine        engine;
    private final AtomicBoolean    handlerRead  = new AtomicBoolean();
    private final AtomicBoolean    handlerWrite = new AtomicBoolean();
    private final ByteBuffer       inboundClear;
    private final ByteBuffer       inboundEncrypted;
    private final ByteBuffer       outboundEncrypted;
    private final SSLSession       session;
    private SSLEngineResult.Status status;
    private final TlsSocketChannel tlsChannel;

    TlsSocketChannelHandler(CommunicationsHandler eventHandler,
                            ChannelHandler handler, SocketChannel channel,
                            int index, SSLEngine engine,
                            ByteBuffer inboundClear,
                            ByteBuffer outboundEncrypted,
                            ByteBuffer inboundEncrypted) {
        super(eventHandler, handler, channel, index);
        tlsChannel = new TlsSocketChannel(this);
        this.engine = engine;
        this.inboundClear = inboundClear;
        this.inboundEncrypted = inboundEncrypted;
        this.outboundEncrypted = outboundEncrypted;
        session = engine.getSession();
    }

    /**
    *
    */
    @Override
    public void close() {
        if (!open.get()) {
            return;
        }
        engine.closeOutbound();
        if (outboundEncrypted.hasRemaining()) {
            log.trace(String.format("There is some data left to be sent on: %s, waiting for close: %s [%s]",
                                    channel, outboundEncrypted,
                                    handler.getName()));
        } else {
            doShutdown();
        }
    }

    @Override
    public SocketChannel getChannel() {
        return tlsChannel;
    }

    @Override
    public SSLSession getSslSession() {
        return session;
    }

    @Override
    public void selectForRead() {
        if (handlerRead.compareAndSet(false, true)) {
            if (inboundClear.hasRemaining()) {
                super.handleRead();
            } else {
                if (inboundEncrypted.position() == 0
                    || status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                    super.selectForRead();
                } else {
                    try {
                        if (readAndUnwrap() == 0) {
                            super.selectForRead();
                        } else {
                            super.handleRead();
                        }
                    } catch (IOException e) {
                        log.error(String.format("Error unwrapping encrypted inbound data on: %s [%s]",
                                                channel, handler.getName()), e);
                        close();
                    }
                }
            }
        }
    }

    @Override
    public void selectForWrite() {
        if (handlerWrite.compareAndSet(false, true)) {
            if (!outboundEncrypted.hasRemaining()) {
                super.selectForWrite();
            }
        }
    }

    private void doShutdown() {
        assert !outboundEncrypted.hasRemaining() : String.format("Buffer was not empty. [%s]",
                                                                 handler.getName());
        if (engine.isOutboundDone()) {
            log.trace(String.format("Outbound data is finished. Closing socket %s [%s]",
                                    channel, handler.getName()));
            super.close();
            return;
        }

        // The engine has more things to send 
        /*
         * By RFC 2616, we can "fire and forget" our close_notify
         * message, so that's what we'll do here.
         */
        outboundEncrypted.clear();
        try {
            SSLEngineResult res = engine.wrap(ByteBuffer.allocate(0),
                                              outboundEncrypted);
            log.trace(String.format("Wrapping: %s : %s, [%s]", channel, res,
                                    handler.getName()));
        } catch (SSLException e) {
            log.warn(String.format("Error during shutdown: %s [%s]", channel,
                                   handler.getName()), e);
            close();
            return;
        }
        outboundEncrypted.flip();
        try {
            flushData();
        } catch (IOException e) {
            log.warn(String.format("Error during shutdown flush of data: %s [%s]",
                                   channel, handler.getName()), e);
            super.close();
        }
    }

    private boolean flushData() throws IOException {
        assert outboundEncrypted.hasRemaining() : String.format("Trying to write but outbound encrypted buffer is empty [%s]",
                                                                handler.getName());
        int written;
        try {
            written = channel.write(outboundEncrypted);
        } catch (IOException e) {
            outboundEncrypted.position(outboundEncrypted.limit());
            log.error(String.format("Error flushing data: %s [%s]", channel,
                                    handler.getName()), e);
            throw e;
        }
        log.trace(String.format("Wrote %s bytes to socket: %s [%s]", written,
                                channel, handler.getName()));
        if (outboundEncrypted.hasRemaining()) {
            super.selectForWrite();
            return false;
        } else {
            return true;
        }
    }

    private int readAndUnwrap() throws IOException {
        assert !inboundClear.hasRemaining() : String.format("Application buffer not empty [%s]",
                                                            handler.getName());
        int bytesRead = channel.read(inboundEncrypted);
        log.trace(String.format("Read %s bytes from socket: %s [%s]",
                                bytesRead, channel, handler.getName()));
        if (bytesRead == -1) {
            engine.closeInbound();
            if (inboundEncrypted.position() == 0
                || status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                return -1;
            }
        }

        inboundClear.clear();

        inboundEncrypted.flip();
        SSLEngineResult res;
        do {
            res = engine.unwrap(inboundEncrypted, inboundClear);
            log.info(String.format("Unwrapping: %s [%s]", res,
                                   handler.getName()));
        } while (res.getStatus() == SSLEngineResult.Status.OK
                 && res.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_UNWRAP
                 && res.bytesProduced() == 0);

        if (inboundClear.position() == 0
            && res.getStatus() == SSLEngineResult.Status.OK
            && inboundEncrypted.hasRemaining()) {
            res = engine.unwrap(inboundEncrypted, inboundClear);
            log.info(String.format("Unwrapping: %s [%s]", res,
                                   handler.getName()));
        }

        status = res.getStatus();
        assert status != SSLEngineResult.Status.BUFFER_OVERFLOW : String.format("Buffer should not overflow: ",
                                                                                res,
                                                                                handler.getName());

        if (status == SSLEngineResult.Status.CLOSED) {
            log.trace(String.format("%s is being closed by peer [%s]", channel,
                                    handler.getName()));
            close();
            return -1;
        }

        inboundEncrypted.compact();
        inboundClear.flip();

        return inboundClear.remaining();
    }

    @Override
    void handleRead() {
        assert handlerRead.get() : String.format("Trying to read when there is no read interest set [%s]",
                                                 handler.getName());
        try {
            if (!open.get()) {
                doShutdown();
            } else {
                assert handlerRead.get() : String.format("handleRead() called without read interest being set [%s]",
                                                         handler.getName());

                int bytesUnwrapped = readAndUnwrap();
                if (bytesUnwrapped == -1) {
                    assert engine.isInboundDone() : String.format("End of stream but engine inbound is not closed %s [%s]",
                                                                  channel,
                                                                  handler.getName());
                    close();
                } else if (bytesUnwrapped == 0) {
                    super.selectForRead();
                } else {
                    handlerRead.set(false);
                    super.handleRead();
                }
            }
        } catch (IOException e) {
            log.error(String.format("Error inbound tls read on: %s [%s]",
                                    channel, handler.getName()), e);
            close();
        }
    }

    /**
     * Called from the selector thread.
     */
    @Override
    void handleWrite() {
        try {
            if (flushData()) {
                if (!open.get()) {
                    doShutdown();
                } else {
                    if (handlerWrite.compareAndSet(true, false)) {
                        super.handleWrite();
                    }
                }
            }
        } catch (IOException e) {
            close();
        }
    }

    /**
     * @param dst
     * @return
     * @throws IOException
     */
    int read(ByteBuffer dst) throws IOException {
        return (int) read(new ByteBuffer[] { dst }, dst.position(),
                          dst.remaining());
    }

    /**
     * @param dsts
     * @param offset
     * @param length
     * @return
     * @throws IOException
     */
    long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        if (engine.isInboundDone()) {
            return -1;
        }

        if (!inboundClear.hasRemaining()) {
            int read = readAndUnwrap();
            if (read == -1 || read == 0) {
                return read;
            }
        }

        int totalRead = 0;
        for (ByteBuffer dst : dsts) {
            if (!dst.hasRemaining()) {
                continue;
            }
            int available = inboundClear.remaining();
            if (available <= dst.remaining()) {
                dst.put(inboundClear);
            } else {
                int oldLimit = inboundClear.limit();
                inboundClear.limit(inboundClear.position() + dst.remaining());
                dst.put(inboundClear);
                inboundClear.limit(oldLimit);
            }
            totalRead += available - inboundClear.remaining();
            if (!inboundClear.hasRemaining()) {
                return totalRead;
            }
        }
        return totalRead;
    }

    /**
     * @param src
     * @return
     * @throws IOException
     */
    int write(ByteBuffer src) throws IOException {
        return (int) write(new ByteBuffer[] { src }, src.position(),
                           src.remaining());
    }

    /**
     * @param srcs
     * @param offset
     * @param length
     * @return
     * @throws IOException
     */
    long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        if (outboundEncrypted.hasRemaining()) {
            return 0;
        }

        outboundEncrypted.clear();
        SSLEngineResult res = engine.wrap(srcs, outboundEncrypted);
        log.info(String.format("Wrapping: %s [%s]", res, handler.getName()));
        outboundEncrypted.flip();
        flushData();

        return res.bytesConsumed();
    }
}
