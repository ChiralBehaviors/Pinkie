package com.hellblazer.pinkie.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;

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
            if (handler != null) {
                throw new IllegalStateException("Handler has already been set");
            }
            this.handler = handler;
            if (log.isDebugEnabled()) {
                log.debug("socket {} accepted", this.handler.getChannel());
            }
            protocol.accepted(BufferProtocol.this);
        }

        @Override
        public void closing() {
            if (log.isDebugEnabled()) {
                log.debug("socket {} closing", this.handler.getChannel());
            }
            protocol.closing(BufferProtocol.this);
        }

        @Override
        public void connect(SocketChannelHandler handler) {
            if (handler != null) {
                throw new IllegalStateException("Handler has already been set");
            }
            this.handler = handler;
            if (log.isDebugEnabled()) {
                log.debug("socket {} connected", this.handler.getChannel());
            }
            protocol.connected(BufferProtocol.this);
        }

        @Override
        public void readReady() {
            try {
                int read = handler.getChannel().read(readBuffer);
                if (log.isDebugEnabled()) {
                    log.debug("socket {} read {} bytes", handler.getChannel(),
                              read);
                }
            } catch (IOException e) {
                handler.close();
                if (log.isDebugEnabled()) {
                    log.debug("socket {} errored during read",
                              this.handler.getChannel(), e);
                }
                protocol.readError();
                return;
            }
            if (readBuffer.remaining() == 0) {
                if (log.isDebugEnabled()) {
                    log.debug("socket {} read buffer filled",
                              this.handler.getChannel());
                }
                protocol.readReady();
            } else {
                handler.selectForRead();
            }
        }

        public void selectForRead() {
            handler.selectForRead();
        }

        public void selectForWrite() {
            handler.selectForWrite();
        }

        @Override
        public void writeReady() {
            try {
                int written = handler.getChannel().write(readBuffer);
                if (log.isDebugEnabled()) {
                    log.debug("socket {} wrote {} bytes", handler.getChannel(),
                              written);
                }
            } catch (IOException e) {
                handler.close();
                if (log.isDebugEnabled()) {
                    log.debug("socket {} errored during write",
                              this.handler.getChannel(), e);
                }
                protocol.readError();
                return;
            }
            if (readBuffer.remaining() == 0) {
                if (log.isDebugEnabled()) {
                    log.debug("socket {} write buffer emptied",
                              this.handler.getChannel());
                }
                protocol.writeReady();
            } else {
                handler.selectForRead();
            }
        }

    }

    private static final Logger log = LoggerFactory.getLogger(BufferProtocol.class);

    private CommsHandler                handler;
    private final ByteBuffer            readBuffer;
    private final ByteBuffer            writeBuffer;
    private final BufferProtocolHandler protocol;

    public BufferProtocol(ByteBuffer readBuffer,
                          BufferProtocolHandler protocol, ByteBuffer writeBuffer) {
        this.readBuffer = readBuffer;
        this.protocol = protocol;
        this.writeBuffer = writeBuffer;
        this.handler = new CommsHandler();
    }

    /**
     * @return the CommunicationsHandler for this protocol
     */
    CommunicationsHandler getHandler() {
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
}
