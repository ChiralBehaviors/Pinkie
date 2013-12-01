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
public class TlsSocketChannelHandler extends SocketChannelHandler {
    private static final Logger                                    log            = LoggerFactory.getLogger(TlsSocketChannelHandler.class);
    private final boolean                                          client;
    private ByteBuffer                                             dummy          = ByteBuffer.allocate(0);
    private final SSLEngine                                        engine;
    private final AtomicBoolean                                    handlerRead    = new AtomicBoolean();
    private final AtomicBoolean                                    handlerWrite   = new AtomicBoolean();
    private boolean                                                handshake      = true;
    private final AtomicReference<SSLEngineResult.HandshakeStatus> hsStatus       = new AtomicReference<>();
    private final ByteBuffer                                       inboundClear;
    private final ByteBuffer                                       inboundEncrypted;
    private final ByteBuffer                                       outboundEncrypted;
    private boolean                                                pendingConnect = true;
    private final SSLSession                                       session;
    private SSLEngineResult.Status                                 status;
    private final TlsSocketChannel                                 tlsChannel;

    /**
     * @param eventHandler
     * @param handler
     * @param channel
     * @param index
     */
    public TlsSocketChannelHandler(CommunicationsHandler eventHandler,
                                   ChannelHandler handler,
                                   SocketChannel channel, int index,
                                   SSLEngine engine, boolean client) {
        super(eventHandler, handler, channel, index);
        tlsChannel = new TlsSocketChannel(this);
        this.client = client;
        this.engine = engine;
        this.engine.setUseClientMode(client);
        session = engine.getSession();
        inboundEncrypted = ByteBuffer.allocateDirect(session.getPacketBufferSize() + 50);
        inboundClear = ByteBuffer.allocateDirect(session.getApplicationBufferSize() + 50);
        outboundEncrypted = ByteBuffer.allocateDirect(session.getPacketBufferSize());
        log.trace(String.format("%s peerNetData: %s, peerAppData: %s, netData: %s",
                                channel, inboundEncrypted.capacity(),
                                inboundClear.capacity(),
                                outboundEncrypted.capacity()));
        inboundClear.position(inboundClear.limit());
        outboundEncrypted.position(outboundEncrypted.limit());
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
            log.trace(String.format("There is some data left to be sent on: %s, waiting for close: ",
                                    channel, outboundEncrypted));
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
            assert !handshake : "We should not be handshaking!";
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
                        log.error(String.format("Error unwrapping encrypted inbound data on: %s",
                                                channel), e);
                        close();
                    }
                }
            }
        }
    }

    @Override
    public void selectForWrite() {
        if (handlerWrite.compareAndSet(false, true)) {
            assert !handshake : "We should not be handshaking!";
            if (!outboundEncrypted.hasRemaining()) {
                super.selectForWrite();
            }
        }
    }

    private void doShutdown() {
        assert !outboundEncrypted.hasRemaining() : "Buffer was not empty.";
        if (engine.isOutboundDone()) {
            log.trace(String.format("Outbound data is finished. Closing socket %s",
                                    channel));
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
            SSLEngineResult res = engine.wrap(dummy, outboundEncrypted);
            log.info(String.format("Wrapping: %s : %s", channel, res));
        } catch (SSLException e) {
            log.warn(String.format("Error during shutdown: %s", channel), e);
            close();
            return;
        }
        outboundEncrypted.flip();
        try {
            flushData();
        } catch (IOException e) {
            log.warn(String.format("Error during shutdown flush of data: %s",
                                   channel), e);
            super.close();
        }
    }

    private void finishInitialHandshake() {
        handshake = false;
        if (pendingConnect) {
            pendingConnect = false;
            if (client) {
                super.handleConnect();
            } else {
                super.handleAccept();
            }
        }
    }

    private boolean flushData() throws IOException {
        assert outboundEncrypted.hasRemaining() : "Trying to write but netData buffer is empty";
        int written;
        try {
            written = channel.write(outboundEncrypted);
        } catch (IOException e) {
            outboundEncrypted.position(outboundEncrypted.limit());
            log.error(String.format("Error flushing data: %s", channel), e);
            throw e;
        }
        log.trace(String.format("Wrote %s bytes to socket: ", written, channel));
        if (outboundEncrypted.hasRemaining()) {
            super.selectForWrite();
            return false;
        } else {
            return true;
        }
    }

    private void handshake() {
        while (true) {
            log.trace(String.format("%s handshake status: %s", channel,
                                    hsStatus.get()));
            switch (hsStatus.get()) {
                case FINISHED:
                    if (handshake) {
                        finishInitialHandshake();
                    }
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
                        log.error(String.format("Error during initial handshake on: %s",
                                                channel), e);
                        close();
                        return;
                    }
                    if (handshake
                        && status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                        super.selectForRead();
                    }
                    return;
                case NEED_WRAP:
                    if (outboundEncrypted.hasRemaining()) {
                        return;
                    }

                    outboundEncrypted.clear();
                    SSLEngineResult res;
                    try {
                        res = engine.wrap(dummy, outboundEncrypted);
                    } catch (SSLException e) {
                        log.error(String.format("Error wrapping outbound data during initial handshake on: %s",
                                                channel), e);
                        close();
                        return;
                    }
                    log.info("Wrapping:\n" + res);
                    assert res.bytesProduced() != 0 : "No net data produced during handshake wrap.";
                    assert res.bytesConsumed() == 0 : "App data consumed during handshake wrap.";
                    hsStatus.set(engine.getHandshakeStatus());
                    outboundEncrypted.flip();

                    try {
                        if (!flushData()) {
                            return;
                        }
                    } catch (IOException e) {
                        log.error(String.format("Error flushing data during initial handshake on: %s",
                                                channel), e);
                        close();
                        return;
                    }
                    break;
                case NOT_HANDSHAKING:
                    assert false : "doHandshake() should never reach the NOT_HANDSHAKING state";
                    return;
            }
        }
    }

    private int readAndUnwrap() throws IOException {
        assert !inboundClear.hasRemaining() : "Application buffer not empty";
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

        inboundClear.clear();

        inboundEncrypted.flip();
        SSLEngineResult res;
        do {
            res = engine.unwrap(inboundEncrypted, inboundClear);
            log.info("Unwrapping:\n" + res);
        } while (res.getStatus() == SSLEngineResult.Status.OK
                 && res.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_UNWRAP
                 && res.bytesProduced() == 0);

        if (res.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED) {
            finishInitialHandshake();
        }

        if (inboundClear.position() == 0
            && res.getStatus() == SSLEngineResult.Status.OK
            && inboundEncrypted.hasRemaining()) {
            res = engine.unwrap(inboundEncrypted, inboundClear);
            log.info("Unwrapping:\n" + res);
        }

        status = res.getStatus();
        hsStatus.set(engine.getHandshakeStatus());
        assert status != SSLEngineResult.Status.BUFFER_OVERFLOW : "Buffer should not overflow: "
                                                                  + res.toString();

        if (status == SSLEngineResult.Status.CLOSED) {
            log.trace(String.format("%s is being closed by peer", channel));
            close();
            return -1;
        }

        inboundEncrypted.compact();
        inboundClear.flip();

        HandshakeStatus handshakeStatus = hsStatus.get();
        if (handshakeStatus == SSLEngineResult.HandshakeStatus.NEED_TASK
            || handshakeStatus == SSLEngineResult.HandshakeStatus.NEED_WRAP
            || handshakeStatus == SSLEngineResult.HandshakeStatus.FINISHED) {
            log.trace(String.format("%s rehandshaking: %s", channel,
                                    handshakeStatus));
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
    void handleAccept() {
        try {
            engine.beginHandshake();
        } catch (SSLException e) {
            log.error(String.format("Error beginning handshake on %s", channel),
                      e);
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
            log.error(String.format("Error beginning handshake on %s", channel),
                      e);
            close();
            return;
        }
        hsStatus.set(engine.getHandshakeStatus());
        handshake();
    }

    @Override
    void handleRead() {
        assert handshake || handlerRead.get() : "Trying to read when there is no read interest set";
        try {
            if (handshake) {
                handshake();
            } else if (!open.get()) {
                doShutdown();
            } else {
                assert handlerRead.get() : "handleRead() called without read interest being set";

                int bytesUnwrapped = readAndUnwrap();
                if (bytesUnwrapped == -1) {
                    assert engine.isInboundDone() : "End of stream but engine inbound is not closed";
                    close();
                } else if (bytesUnwrapped == 0) {
                    super.selectForRead();
                } else {
                    handlerRead.set(false);
                    super.handleRead();
                }
            }
        } catch (IOException e) {
            log.error(String.format("Error inbound tls read on: %s", channel),
                      e);
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
                if (handshake) {
                    handshake();
                } else if (!open.get()) {
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
        assert !handshake : "We should not be handshaking!";
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
            dst.put(inboundClear);
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
        assert !handshake : "We should not be handshaking!";

        if (outboundEncrypted.hasRemaining()) {
            return 0;
        }

        outboundEncrypted.clear();
        SSLEngineResult res = engine.wrap(srcs, outboundEncrypted);
        log.info("Wrapping:\n" + res);
        outboundEncrypted.flip();
        flushData();

        return res.bytesConsumed();
    }
}
