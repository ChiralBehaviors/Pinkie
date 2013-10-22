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

import java.nio.ByteBuffer;

/**
 * The handler for buffer protocol events. These methods will be called by the
 * protocol.
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
    void readError(BufferProtocol bufferProtocol);

    /**
     * The readBuffer has been filled
     * 
     */
    void readReady(ByteBuffer readBuffer, BufferProtocol bufferProtocol);

    /**
     * The write buffer has been emptied
     * 
     */
    void writeReady(ByteBuffer writeBuffer, BufferProtocol bufferProtocol);

    /**
     * The buffer protocol produced a write error
     * 
     * @param bufferProtocol
     */
    void writeError(BufferProtocol bufferProtocol);
}
