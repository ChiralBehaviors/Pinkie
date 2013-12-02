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
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
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
public class TlsHandshakeHandler extends SocketChannelHandler {
    private static final Logger                                    log      = LoggerFactory.getLogger(TlsHandshakeHandler.class);
    private final boolean                                          client;
    private final SSLEngine                                        engine;
    private final AtomicReference<SSLEngineResult.HandshakeStatus> hsStatus = new AtomicReference<>();
    private final ByteBuffer                                       inboundClear;
    private final ByteBuffer                                       inboundEncrypted;
    private final ByteBuffer                                       outboundEncrypted;
    private SSLEngineResult.Status                                 status;

    /**
     * @param eventHandler
     * @param handler
     * @param channel
     * @param index
     */
    public TlsHandshakeHandler(CommunicationsHandler eventHandler,
                               ChannelHandler handler, SocketChannel channel,
                               int index, SSLEngine engine, boolean client) {
        super(eventHandler, handler, channel, index);
        this.client = client;
        this.engine = engine;
        this.engine.setUseClientMode(client);
        SSLSession session = engine.getSession();
        inboundClear = ByteBuffer.allocateDirect(session.getApplicationBufferSize() * 4 + 50);
        inboundEncrypted = ByteBuffer.allocateDirect(session.getPacketBufferSize() + 50);
        outboundEncrypted = ByteBuffer.allocateDirect(session.getPacketBufferSize() + 50);
        outboundEncrypted.position(outboundEncrypted.limit());
        inboundClear.position(inboundClear.limit());
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
            log.trace(String.format("There is some data left to be sent on: %s, waiting for close:  [%s]",
                                    channel, outboundEncrypted,
                                    handler.getName()));
        } else {
            doShutdown();
        }
    }

    private void doShutdown() {
        assert !outboundEncrypted.hasRemaining() : String.format("Buffer was not empty [%s]",
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
            log.trace(String.format("Wrapping shutdown: %s : %s [%s]", channel,
                                    res, handler.getName()));
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

    private void finishHandshake() {
        TlsSocketChannelHandler flowHandler = new TlsSocketChannelHandler(
                                                                          getHandler(),
                                                                          handler,
                                                                          channel,
                                                                          index,
                                                                          engine,
                                                                          outboundEncrypted,
                                                                          inboundEncrypted);
        handler.delink(this);
        handler.addHandler(flowHandler);
        if (client) {
            log.trace(String.format("Connecting %s [%s]", channel,
                                    handler.getName()));
            flowHandler.handleConnect();
        } else {
            log.trace(String.format("Accepting %s [%s]", channel,
                                    handler.getName()));
            flowHandler.handleAccept();
        }
    }

    private boolean flushData() throws IOException {
        //        assert outboundEncrypted.hasRemaining() : String.format("Trying to write but outbound encrypted buffer is empty [%s]",
        //                                                                handler.getName());
        int written;
        try {
            written = channel.write(outboundEncrypted);
        } catch (IOException e) {
            outboundEncrypted.position(outboundEncrypted.limit());
            log.error(String.format("Error flushing data: %s [%s]", channel,
                                    handler.getName()), e);
            throw e;
        }
        log.trace(String.format("Wrote %s bytes, remaining: %s to socket:%s  [%s]",
                                written, outboundEncrypted.remaining(),
                                channel, handler.getName()));
        if (outboundEncrypted.hasRemaining()) {
            selectForWrite();
            return false;
        } else {
            return true;
        }
    }

    private void handshake() {
        while (true) {
            log.trace(String.format("%s handshake status: %s [%s]", channel,
                                    hsStatus.get(), handler.getName()));
            switch (hsStatus.get()) {
                case FINISHED:
                    finishHandshake();
                    return;
                case NEED_TASK:
                    Runnable task = engine.getDelegatedTask();
                    if (task != null) {
                        handler.execute(task(task));
                    }
                    return;
                case NEED_UNWRAP:
                    try {
                        readAndUnwrap();
                    } catch (IOException e) {
                        log.error(String.format("Error during initial handshake on: %s [%s]",
                                                channel, handler.getName()), e);
                        close();
                        return;
                    }
                    if (status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                        selectForRead();
                    }
                    return;
                case NEED_WRAP:
                    if (outboundEncrypted.hasRemaining()) {
                        return;
                    }

                    outboundEncrypted.clear();
                    SSLEngineResult res;
                    try {
                        res = engine.wrap(ByteBuffer.allocate(0),
                                          outboundEncrypted);
                    } catch (SSLException e) {
                        log.error(String.format("Error wrapping outbound data during initial handshake on: %s [%s]",
                                                channel, handler.getName()), e);
                        close();
                        return;
                    }
                    log.trace(String.format("Wrapping: %s [%s]", res,
                                            handler.getName()));
                    assert res.bytesProduced() != 0 : String.format("No net data produced during handshake wrap. [%s]",
                                                                    handler.getName());
                    assert res.bytesConsumed() == 0 : String.format("App data consumed during handshake wrap. [%s]",
                                                                    handler.getName());
                    hsStatus.set(engine.getHandshakeStatus());
                    outboundEncrypted.flip();

                    try {
                        if (!flushData()) {
                            return;
                        }
                    } catch (IOException e) {
                        log.error(String.format("Error flushing data during initial handshake on: %s [%s]",
                                                channel, handler.getName()), e);
                        close();
                        return;
                    }
                    break;
                case NOT_HANDSHAKING:
                    finishHandshake();
                    return;
            }
        }
    }

    private int readAndUnwrap() throws IOException {
        int bytesRead = channel.read(inboundEncrypted);
        log.trace(String.format("Read %s bytes from socket: %s", bytesRead,
                                channel));
        if (bytesRead == -1) {
            engine.closeInbound();
            if (inboundEncrypted.position() == 0
                || status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                return -1;
            }
        }
        inboundEncrypted.flip();
        SSLEngineResult res;
        do {
            res = engine.unwrap(inboundEncrypted, inboundClear);
            log.trace(String.format("Unwrapping: %s [%s]", res,
                                    handler.getName()));
        } while (res.getStatus() == SSLEngineResult.Status.OK
                 && res.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_UNWRAP
                 && res.bytesProduced() == 0);

        if (res.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED) {
            finishHandshake();
        }

        if (inboundClear.position() == 0
            && res.getStatus() == SSLEngineResult.Status.OK
            && inboundEncrypted.hasRemaining()) {
            res = engine.unwrap(inboundEncrypted, inboundClear);
            log.trace(String.format("Unwrapping: %s [%s]", res,
                                    handler.getName()));
        }

        status = res.getStatus();
        hsStatus.set(engine.getHandshakeStatus());

        if (status == SSLEngineResult.Status.CLOSED) {
            log.trace(String.format("%s is being closed by peer [%s]", channel,
                                    handler.getName()));
            close();
            return -1;
        }

        inboundEncrypted.compact();
        inboundClear.clear();

        HandshakeStatus handshakeStatus = hsStatus.get();
        if (handshakeStatus == SSLEngineResult.HandshakeStatus.NEED_TASK
            || handshakeStatus == SSLEngineResult.HandshakeStatus.NEED_WRAP
            || handshakeStatus == SSLEngineResult.HandshakeStatus.FINISHED) {
            log.trace(String.format("%s rehandshaking: %s  [%s]", channel,
                                    handshakeStatus, handler.getName()));
            handshake();
        }

        return inboundClear.remaining();
    }

    private Runnable task(final Runnable engineTask) {
        return new Runnable() {
            @Override
            public void run() {
                engineTask.run();
                hsStatus.set(engine.getHandshakeStatus());
                handshake();
            }
        };
    }

    @Override
    Runnable getReadHandler() {
        return new Runnable() {
            @Override
            public void run() {
                handshake();
            }
        };
    }

    @Override
    Runnable getWriteHandler() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    if (flushData()) {
                        handshake();
                    }
                } catch (IOException e) {
                    close();
                }
            }
        };
    }

    @Override
    void handleAccept() {
        try {
            engine.beginHandshake();
        } catch (SSLException e) {
            log.error(String.format("Error beginning handshake on %s  [%s]",
                                    channel, handler.getName()), e);
            close();
            return;
        }
        hsStatus.set(engine.getHandshakeStatus());
        handshake();
    }

    @Override
    void handleConnect() {
        try {
            engine.beginHandshake();
        } catch (SSLException e) {
            log.error(String.format("Error beginning handshake on %s [%s]",
                                    channel, handler.getName()), e);
            close();
            return;
        }
        hsStatus.set(engine.getHandshakeStatus());
        handshake();
    }
}
