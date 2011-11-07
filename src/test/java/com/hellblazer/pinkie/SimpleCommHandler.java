/*
 * Copyright (c) 2009, 2011 Hal Hildebrand, all rights reserved.
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class SimpleCommHandler implements CommunicationsHandler {

    final AtomicBoolean                         accepted  = new AtomicBoolean();
    final AtomicBoolean                         connected = new AtomicBoolean();
    final AtomicReference<SocketChannelHandler> handler   = new AtomicReference<SocketChannelHandler>();
    final List<byte[]>                          reads     = new ArrayList<byte[]>();
    final List<ByteBuffer>                      writes    = new CopyOnWriteArrayList<ByteBuffer>();

    @Override
    public void closing() {
    }

    @Override
    public void accept(SocketChannelHandler handler) {
        this.handler.set(handler);
        accepted.set(true);
    }

    @Override
    public void connect(SocketChannelHandler handler) {
        connected.set(true);
    }

    @Override
    public void readReady() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ByteBuffer buffer = ByteBuffer.wrap(new byte[1024]);
            for (int read = handler.get().getChannel().read(buffer); read != 0; read = handler.get().getChannel().read(buffer)) {
                buffer.flip();
                byte[] b = new byte[read];
                buffer.get(b, 0, read);
                baos.write(b);
                buffer.flip();
            }
            reads.add(baos.toByteArray());
        } catch (Throwable e) {
            throw new IllegalStateException(e);
        }
        handler.get().selectForRead();
    }

    @Override
    public void writeReady() {
        if (writes.size() == 0) {
            return;
        }
        try {
            ByteBuffer buffer = writes.get(0);
            handler.get().getChannel().write(buffer);
            if (!buffer.hasRemaining()) {
                writes.remove(0);
            } else {
                handler.get().selectForWrite();
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void selectForRead() {
        handler.get().selectForRead();
    }

    public void selectForWrite() {
        handler.get().selectForWrite();
    }
}
