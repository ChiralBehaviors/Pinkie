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
import java.util.concurrent.atomic.AtomicReference;

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
    private static final Logger                           log                     = LoggerFactory.getLogger(TlsSocketChannelHandler.class);

    private final SSLEngine                               engine;
    private final AtomicBoolean                           flushing                = new AtomicBoolean();
    private final ByteBuffer                              inboundEncrypted;
    private final ByteBuffer                              outboundEncrypted;
    private final AtomicBoolean                           selectForWriteRequested = new AtomicBoolean();
    private final SSLSession                              session;
    private final AtomicReference<SSLEngineResult.Status> status                  = new AtomicReference<>();
    private final TlsSocketChannel                        tlsChannel;

    TlsSocketChannelHandler(CommunicationsHandler eventHandler,
                            ChannelHandler handler, SocketChannel channel,
                            int index, SSLEngine engine,
                            ByteBuffer outboundEncrypted,
                            ByteBuffer inboundEncrypted) {
        super(eventHandler, handler, channel, index);
        tlsChannel = new TlsSocketChannel(this);
        this.engine = engine;
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
            shutdown();
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
    public void selectForWrite() {
        if (!flushing.get()) {
            selectForWriteRequested.set(true);
            super.selectForWrite();
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
            log.error(String.format("Error flushing encrypted data: %s [%s]",
                                    channel, handler.getName()), e);
            throw e;
        }
        log.trace(String.format("Wrote %s encrypted bytes to socket: %s [%s]",
                                written, channel, handler.getName()));
        if (outboundEncrypted.hasRemaining()) {
            super.selectForWrite();
            return false;
        } else {
            return true;
        }
    }

    private int readAndUnwrap(ByteBuffer[] dsts) throws IOException {
        int bytesRead = channel.read(inboundEncrypted);
        log.trace(String.format("Read %s bytes from socket: %s [%s]",
                                bytesRead, channel, handler.getName()));
        if (bytesRead == -1) {
            engine.closeInbound();
            if (inboundEncrypted.position() == 0
                || status.get() == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                return -1;
            }
        }
        SSLEngineResult res;
        int totalRead = 0;
        do {
            res = engine.unwrap(inboundEncrypted, dsts);
            log.info(String.format("Unwrapping: %s [%s]", res,
                                   handler.getName()));
            totalRead += res.bytesProduced();
        } while (res.getStatus() == SSLEngineResult.Status.OK
                 && res.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_UNWRAP
                 && res.bytesProduced() == 0);

        if (res.getStatus() == SSLEngineResult.Status.OK
            && inboundEncrypted.hasRemaining()) {
            res = engine.unwrap(inboundEncrypted, dsts);
            log.info(String.format("Unwrapping: %s [%s]", res,
                                   handler.getName()));
            totalRead += res.bytesProduced();
        }

        status.set(res.getStatus());

        if (res.getStatus() == SSLEngineResult.Status.CLOSED) {
            log.trace(String.format("%s is being closed by peer [%s]", channel,
                                    handler.getName()));
            return -1;
        }

        inboundEncrypted.compact();
        return totalRead;
    }

    private void shutdown() {
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

    @Override
    Runnable getWriteHandler() {
        return new Runnable() {
            @Override
            public void run() {
                if (flushing.compareAndSet(true, false)) {
                    try {
                        if (!flushData()) {
                            return;
                        }
                    } catch (IOException e) {
                        log.warn(String.format("Error during flush of encrypted data: %s [%s]",
                                               channel, handler.getName()), e);
                        tlsChannel.setDeferredException(e);
                        close();
                        return;
                    }
                }
                if (selectForWriteRequested.compareAndSet(true, false)) {
                    eventHandler.writeReady();
                }
            }
        };
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

        return readAndUnwrap(dsts);
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
