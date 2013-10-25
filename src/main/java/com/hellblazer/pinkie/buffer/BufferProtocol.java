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
package com.hellblazer.pinkie.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;

/**
 * The implementation of a simple bidirectional non blocking binary buffer
 * protocol
 * 
 * @author hhildebrand
 * 
 */
final public class BufferProtocol {

    private class CommsHandler implements CommunicationsHandler {

        private SocketChannelHandler handler;

        @Override
        public void accept(SocketChannelHandler handler) {
            if (this.handler != null) {
                throw new IllegalStateException("Handler has already been set");
            }
            this.handler = handler;
            if (log.isDebugEnabled()) {
                log.debug("socket {} accepted", socketInfo());
            }
            protocol.accepted(BufferProtocol.this);
        }

        public void close() {
            handler.close();
        }

        @Override
        public void closing() {
            if (log.isDebugEnabled()) {
                log.debug("socket {} closing", socketInfo());
            }
            protocol.closing();
        }

        @Override
        public void connect(SocketChannelHandler handler) {
            if (this.handler != null) {
                throw new IllegalStateException("Handler has already been set");
            }
            this.handler = handler;
            if (log.isDebugEnabled()) {
                log.debug("socket {} connected", this.socketInfo());
            }
            protocol.connected(BufferProtocol.this);
        }

        @Override
        public void readReady() {
            try {
                if (log.isDebugEnabled()) {
                    log.debug("socket {} still need to read {} bytes",
                              socketInfo(), readBuffer.remaining());
                }
                int read = handler.getChannel().read(readBuffer);
                if (log.isDebugEnabled()) {
                    log.debug("socket {} read {} bytes", socketInfo(), read);
                }
            } catch (IOException e) {
                if (isClosedConnection(e)) {
                    if (log.isDebugEnabled()) {
                        log.debug("socket {} closed during read", socketInfo());
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("socket {} errored during read",
                                  socketInfo(), e);
                    }
                    protocol.readError();
                }
                handler.close();
                return;
            }
            if (readBuffer.hasRemaining()) {
                if (readFullBuffer) {
                    if (log.isDebugEnabled()) {
                        log.debug("socket {} read buffer still needs bytes, selecting for read",
                                  socketInfo());
                    }
                    handler.selectForRead();
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("socket {} read buffer partial read",
                                  socketInfo());
                    }
                    protocol.readReady(readBuffer);
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("socket {} read buffer filled", socketInfo());
                }
                protocol.readReady(readBuffer);
            }
        }

        public void selectForRead() {
            handler.selectForRead();
        }

        public void selectForWrite() {
            handler.selectForWrite();
        }

        public String socketInfo() {
            SocketChannel channel = handler.getChannel();
            return String.format("[%s local=%s remote=%s]",
                                 channel.isConnected() ? "connected"
                                                      : "unconnected",
                                 channel.socket().getLocalAddress(),
                                 channel.socket().getRemoteSocketAddress());
        }

        @Override
        public void writeReady() {
            try {
                if (log.isDebugEnabled()) {
                    log.debug("socket {} still need to write {} bytes",
                              socketInfo(), writeBuffer.remaining());
                }
                int written = handler.getChannel().write(writeBuffer);
                if (log.isDebugEnabled()) {
                    log.debug("socket {} wrote {} bytes", socketInfo(), written);
                }
            } catch (IOException e) {
                if (isClosedConnection(e)) {
                    if (log.isDebugEnabled()) {
                        log.debug("socket {} closed during write", socketInfo());
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("socket {} errored during write",
                                  socketInfo(), e);
                    }
                    protocol.writeError();
                }
                handler.close();
                return;
            }
            if (writeBuffer.hasRemaining()) {
                if (writeFullBuffer) {
                    if (log.isDebugEnabled()) {
                        log.debug("socket {} write buffer still has bytes, selecting for write",
                                  socketInfo());
                    }
                    handler.selectForWrite();
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("socket {} write buffer partial write",
                                  socketInfo());
                    }
                    protocol.writeReady(writeBuffer);
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("socket {} write buffer emptied", socketInfo());
                }
                protocol.writeReady(writeBuffer);
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(BufferProtocol.class);

    /**
     * Answer true if the io exception is a form of a closed connection
     * 
     * @param ioe
     * @return
     */
    public static boolean isClosedConnection(IOException ioe) {
        return ioe instanceof ClosedChannelException
               || "Broken pipe".equals(ioe.getMessage())
               || "Connection reset by peer".equals(ioe.getMessage());
    }

    private CommsHandler                handler;
    private final ByteBuffer            readBuffer;
    private final ByteBuffer            writeBuffer;
    private boolean                     writeFullBuffer = true;
    private boolean                     readFullBuffer  = true;

    private final BufferProtocolHandler protocol;

    public BufferProtocol(BufferProtocolHandler protocol) {
        this.readBuffer = protocol.newReadBuffer();
        this.protocol = protocol;
        this.writeBuffer = protocol.newWriteBuffer();
        this.handler = new CommsHandler();
    }

    public void close() {
        handler.close();
    }

    /**
     * @return the CommunicationsHandler for this protocol
     */
    public CommunicationsHandler getHandler() {
        return handler;
    }

    /**
     * 
     * @return the buffer used to read
     */
    public ByteBuffer getReadBuffer() {
        return readBuffer;
    }

    /**
     * 
     * @return the buffer used to write
     */
    public ByteBuffer getWriteBuffer() {
        return writeBuffer;
    }

    /**
     * kick off the reading of the socket's inbound stream into the read buffer
     */
    public void selectForRead() {
        handler.selectForRead();
    }

    /**
     * kick off the writing to the socket's outbound stream from the write
     * buffer
     */
    public void selectForWrite() {
        handler.selectForWrite();
    }

    /**
     * Set the behavior of the asynchronous read.
     * 
     * @param readFullBuffer
     *            - true if the read buffer must be filled to get a callback,
     *            false if callbacks are sent for every non zero read
     */
    public void setReadFullBuffer(boolean readFullBuffer) {
        this.readFullBuffer = readFullBuffer;
    }

    /**
     * Set the behavior of the asynchronous write.
     * 
     * @param writeFullBuffer
     *            - true if the write buffer must be emptied to get a callback,
     *            false if callbacks are sent for every non zero write
     */
    public void setWriteFullBuffer(boolean writeFullBuffer) {
        this.writeFullBuffer = writeFullBuffer;
    }
}
