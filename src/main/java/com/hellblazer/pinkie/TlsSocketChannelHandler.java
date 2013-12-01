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
 * @author hhildebrand
 * 
 */
public class TlsSocketChannelHandler extends SocketChannelHandler {
    private static final Logger                                    log            = LoggerFactory.getLogger(TlsSocketChannelHandler.class);
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
    private final boolean                                          server;
    private final SSLSession                                       session;
    private SSLEngineResult.Status                                 status;

    /**
     * @param eventHandler
     * @param handler
     * @param channel
     * @param index
     */
    public TlsSocketChannelHandler(CommunicationsHandler eventHandler,
                                   ChannelHandler handler,
                                   SocketChannel channel, int index,
                                   SSLEngine engine, boolean server) {
        super(eventHandler, handler, channel, index);
        this.server = server;
        this.engine = engine;
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
    public void selectForRead() {
        if (handlerRead.compareAndSet(false, true)) {
            if (handshake) {
                // Wait for handshake to finish
                return;
            } else {
                if (inboundClear.hasRemaining()) {
                    super.handleRead();
                } else {
                    // There is no decrypted data. But there may be some 
                    // encrypted data.
                    if (inboundEncrypted.position() == 0
                        || status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                        // Must read more data since either there is no encrypted
                        // data available or there is data available but not
                        // enough to reassemble a packet.
                        super.selectForRead();
                    } else {
                        // There is encrypted data available. It may or may not
                        // be enough to reassemble a full packet. We have to check it.
                        try {
                            if (readAndUnwrap() == 0) {
                                // Not possible to reassemble a full packet.
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
    }

    @Override
    public void selectForWrite() {
        if (handlerWrite.compareAndSet(false, true)) {
            if (handshake) {
                return;
            } else {
                if (!outboundEncrypted.hasRemaining()) {
                    super.selectForWrite();
                }
            }
        }
    }

    private void doShutdown() {
        assert !outboundEncrypted.hasRemaining() : "Buffer was not empty.";
        // Either shutdown was initiated now or we are on the middle
        // of shutting down and this method was called after emptying 
        // the out buffer

        // If the engine has nothing else to do, close the socket. If
        // this socket is dead because of an exception, close it
        // immediately
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
            // Problems with the engine. Probably it is dead. So close 
            // the socket and forget about it. 
            log.warn(String.format("Error during shutdown: %s", channel), e);
            close();
            return;
        }
        outboundEncrypted.flip();
        try {
            flushData();
        } catch (IOException e) {
            super.close();
        }
    }

    private void finishInitialHandshake() {
        handshake = false;
        if (pendingConnect) {
            pendingConnect = false;
            if (server) {
                super.handleAccept();
            } else {
                super.handleConnect();
            }
        }
    }

    /**
     * Tries to write the data on the netData buffer to the socket. If not all
     * data is sent, the write interest is activated with the selector thread.
     * 
     * @return True if all data was sent. False otherwise.
     * @throws IOException
     */
    private boolean flushData() throws IOException {
        assert outboundEncrypted.hasRemaining() : "Trying to write but netData buffer is empty";
        int written;
        try {
            written = channel.write(outboundEncrypted);
        } catch (IOException e) {
            // Clear the buffer. If write failed, the socket is dead. Clearing
            // the buffer indicates that no more write should be attempted.
            outboundEncrypted.position(outboundEncrypted.limit());
            log.error(String.format("Error flushing data: %s", channel), e);
            throw e;
        }
        log.trace(String.format("Wrote %s bytes to socket: ", written, channel));
        if (outboundEncrypted.hasRemaining()) {
            // The buffer is not empty. Register again with the selector
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
                    Runnable task;
                    while ((task = engine.getDelegatedTask()) != null) {
                        handler.execute(task(task));
                    }
                    // The hs status was updated, so go back to the switch
                    break;

                case NEED_UNWRAP:
                    try {
                        readAndUnwrap();
                    } catch (IOException e) {
                        log.error(String.format("Error during initial handshake on: %s",
                                                channel), e);
                        close();
                        return;
                    }
                    // During normal operation a call to readAndUnwrap() that results in underflow
                    // does not cause the channel to activate read interest with the selector.
                    // Therefore, if we are at the initial handshake, we must activate the read
                    // insterest explicitily.
                    if (handshake
                        && status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                        super.selectForRead();
                    }
                    return;

                case NEED_WRAP:
                    // First make sure that the out buffer is completely empty. Since we
                    // cannot call wrap with data left on the buffer
                    if (outboundEncrypted.hasRemaining()) {
                        return;
                    }

                    // Prepare to write
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

                    // Now send the data and come back here only when 
                    // the data is all sent
                    try {
                        if (!flushData()) {
                            // There is data left to be send. Wait for it
                            return;
                        }
                    } catch (IOException e) {
                        log.error(String.format("Error flushing data during initial handshake on: %s",
                                                channel), e);
                        close();
                        return;
                    }
                    // All data was sent. Break from the switch but don't 
                    // exit this method. It will loop again, since there may be more
                    // operations that can be done without blocking.
                    break;

                case NOT_HANDSHAKING:
                    assert false : "doHandshake() should never reach the NOT_HANDSHAKING state";
                    return;
            }
        }
    }

    private int readAndUnwrap() throws IOException {
        assert !inboundClear.hasRemaining() : "Application buffer not empty";
        // No decrypted data left on the buffers.
        // Try to read from the socket. There may be some data
        // on the peerNetData buffer, but it might not be sufficient.       
        int bytesRead = channel.read(inboundEncrypted);
        log.trace(String.format("Read %s bytes from socket: %s", bytesRead,
                                channel));
        if (bytesRead == -1) {
            // We will not receive any more data. Closing the engine
            // is a signal that the end of stream was reached.
            engine.closeInbound();
            // EOF. But do we still have some useful data available? 
            if (inboundEncrypted.position() == 0
                || status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                // Yup. Either the buffer is empty or it's in underflow,
                // meaning that there is not enough data to reassemble a
                // TLS packet. So we can return EOF.
                return -1;
            }
            // Although we reach EOF, we still have some data left to
            // be decrypted. We must process it 
        }

        // Prepare the application buffer to receive decrypted data
        inboundClear.clear();

        // Prepare the net data for reading. 
        inboundEncrypted.flip();
        SSLEngineResult res;
        do {
            res = engine.unwrap(inboundEncrypted, inboundClear);
            log.info("Unwrapping:\n" + res);
            // During an handshake renegotiation we might need to perform
            // several unwraps to consume the handshake data.
        } while (res.getStatus() == SSLEngineResult.Status.OK
                 && res.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_UNWRAP
                 && res.bytesProduced() == 0);

        // If the initial handshake finish after an unwrap, we must activate
        // the application interestes, if any were set during the handshake
        if (res.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED) {
            finishInitialHandshake();
        }

        // If no data was produced, and the status is still ok, try to read once more
        if (inboundClear.position() == 0
            && res.getStatus() == SSLEngineResult.Status.OK
            && inboundEncrypted.hasRemaining()) {
            res = engine.unwrap(inboundEncrypted, inboundClear);
            log.info("Unwrapping:\n" + res);
        }

        /*
         * The status may be:
         * OK - Normal operation
         * OVERFLOW - Should never happen since the application buffer is 
         *  sized to hold the maximum packet size.
         * UNDERFLOW - Need to read more data from the socket. It's normal.
         * CLOSED - The other peer closed the socket. Also normal.
         */
        status = res.getStatus();
        hsStatus.set(engine.getHandshakeStatus());
        // Should never happen, the peerAppData must always have enough space
        // for an unwrap operation
        assert status != SSLEngineResult.Status.BUFFER_OVERFLOW : "Buffer should not overflow: "
                                                                  + res.toString();

        // The handshake status here can be different than NOT_HANDSHAKING
        // if the other peer closed the connection. So only check for it
        // after testing for closure.
        if (status == SSLEngineResult.Status.CLOSED) {
            log.trace(String.format("%s is being closed by peer", channel));
            close();
            return -1;
        }

        // Prepare the buffer to be written again.
        inboundEncrypted.compact();
        // And the app buffer to be read.
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
            }
        };
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
                // The read interest is always set when this method is called                   
                assert handlerRead.get() : "handleRead() called without read interest being set";

                int bytesUnwrapped = readAndUnwrap();
                if (bytesUnwrapped == -1) {
                    // End of stream. 
                    assert engine.isInboundDone() : "End of stream but engine inbound is not closed";
                    close();

                } else if (bytesUnwrapped == 0) {
                    // Must read more data
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
                // The buffer was sent completely
                if (handshake) {
                    handshake();

                } else if (!open.get()) {
                    doShutdown();

                } else {
                    // If the listener is interested in writing, 
                    // prepare to fire the event.
                    if (handlerWrite.compareAndSet(true, false)) {
                        super.handleWrite();
                    }
                }
            } else {
                // There is still more data to be sent. Wait for another
                // write event. Calling flush data already resulted in the
                // write interest being reactivated.
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
        if (handshake) {
            return 0;
        }

        // Perhaps we should always try to read some data from
        // the socket. In some situations, it might not be possible
        // to unwrap some of the data stored on the buffers before
        // reading more.

        // Check if the stream is closed.
        if (engine.isInboundDone()) {
            // We reached EOF.
            return -1;
        }

        // First check if there is decrypted data waiting in the buffers
        if (!inboundClear.hasRemaining()) {
            int read = readAndUnwrap();
            if (read == -1 || read == 0) {
                return read;
            }
        }

        int totalRead = 0;
        for (ByteBuffer dst : dsts) {
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
        if (handshake) {
            // Not ready to write
            return 0;
        }

        // First, check if we still have some data waiting to be sent.
        if (outboundEncrypted.hasRemaining()) {
            return 0;
        }
        // There is no data left to be sent. Clear the buffer and get
        // ready to encrypt more data.
        outboundEncrypted.clear();
        SSLEngineResult res = engine.wrap(srcs, outboundEncrypted);
        log.info("Wrapping:\n" + res);
        // Prepare the buffer for reading
        outboundEncrypted.flip();
        flushData();

        // Return the number of bytes read 
        // from the source buffer
        return res.bytesConsumed();
    }
}
