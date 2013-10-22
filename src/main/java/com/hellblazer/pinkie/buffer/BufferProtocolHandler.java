package com.hellblazer.pinkie.buffer;

import java.nio.ByteBuffer;

/**
 * 
 * @author hhildebrand
 * 
 */
public interface BufferProtocolHandler {
    /**
     * The buffer protocol has accepted the inbound socket.
     * 
     * @param bufferProtocol
     */
    void accepted(BufferProtocol bufferProtocol);

    /**
     * The buffer protocol is closing the socket
     * 
     * @param bufferProtocol
     */
    void closing(BufferProtocol bufferProtocol);

    /**
     * The buffer protocol has connected the outbound socket
     * 
     * @param bufferProtocol
     */
    void connected(BufferProtocol bufferProtocol);

    /**
     * Create a new ByteBuffer for read use in the buffer protocol
     * 
     * @return a new ByteBuffer for read use in the buffer protocol
     */
    ByteBuffer newReadBuffer();

    /**
     * Create a new ByteBuffer for write use in the buffer protocol
     * 
     * @return a new ByteBuffer for write use in the buffer protocol
     */
    ByteBuffer newWriteBuffer();

    /**
     * The buffer protocol produced a read error
     */
    void readError();

    /**
     * The readBuffer has been filled
     */
    void readReady();

    /**
     * The write buffer has been emptied
     */
    void writeReady();
}
