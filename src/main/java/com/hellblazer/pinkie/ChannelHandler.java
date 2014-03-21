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

import static java.lang.String.format;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a full featured non blocking NIO handler for selectable
 * channels. In addition, it provides facilities for dealing with outbound
 * connections.
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 * 
 */
public class ChannelHandler {
    private final static Logger           log           = LoggerFactory.getLogger(ChannelHandler.class);

    private final ExecutorService         executor;
    private final ReentrantLock           handlersLock  = new ReentrantLock();
    private final String                  name;
    private final AtomicInteger           nextQueue     = new AtomicInteger();
    private volatile SocketChannelHandler openHandlers;
    private final BlockingDeque<Runnable> registers[];
    private final Selector[]              selectors;
    private final Thread[]                selectorThreads;

    final SocketOptions                   options;
    final AtomicBoolean                   run           = new AtomicBoolean();
    final int                             selectTimeout = 1000;

    /**
     * Construct a new channel handler with a single selector queue
     * 
     * @param handlerName
     *            - the String name used to mark the selection thread
     * @param socketOptions
     *            - the socket options to configure new sockets
     * @param executor
     *            - the executor service to handle I/O and selection events
     * @throws IOException
     *             - if things go pear shaped when opening the selector
     */
    public ChannelHandler(String handlerName, SocketOptions socketOptions,
                          ExecutorService executor) throws IOException {
        this(handlerName, socketOptions, executor, null, null);
    }

    /**
     * Construct a new channel handler
     * 
     * @param handlerName
     *            - the String name used to mark the selection thread
     * @param socketOptions
     *            - the socket options to configure new sockets
     * @param executor
     *            - the executor service to handle I/O and selection events
     * @param selectorQueues
     *            - the number of selectors to use
     * @throws IOException
     *             - if things go pear shaped when opening the selector
     */
    @SuppressWarnings("unchecked")
    public ChannelHandler(String handlerName, SocketOptions socketOptions,
                          ExecutorService executor, int selectorQueues)
                                                                       throws IOException {
        name = handlerName;
        if (selectorQueues <= 0) {
            throw new IllegalArgumentException(
                                               String.format("selectorQueues must be > 0: %s",
                                                             selectorQueues));
        }
        selectors = new Selector[selectorQueues];
        selectorThreads = new Thread[selectorQueues];
        LinkedBlockingDeque<Runnable>[] regs = new LinkedBlockingDeque[selectorQueues];
        regs = new LinkedBlockingDeque[selectorQueues];
        registers = regs;
        this.executor = executor;
        options = socketOptions;

        for (int i = 0; i < selectorQueues; i++) {
            selectorThreads[i] = new Thread(selectorTask(i),
                                            String.format("Selector[%s (%s)]",
                                                          name, i));
            selectorThreads[i].setDaemon(true);
            selectors[i] = Selector.open();
            registers[i] = new LinkedBlockingDeque<>();
        }
    }

    /**
     * Construct a new channel handler with a single selector queue
     * 
     * @param handlerName
     *            - the String name used to mark the selection thread
     * @param socketOptions
     *            - the socket options to configure new sockets
     * @param executor
     *            - the executor service to handle I/O and selection events
     * @param sslContext
     *            - the sslContext to use for SSL sessions
     * @param sslParameters
     *            - SSL parameters to use
     * @throws IOException
     *             - if things go pear shaped when opening the selector
     */
    public ChannelHandler(String handlerName, SocketOptions socketOptions,
                          ExecutorService executor, SSLContext sslContext,
                          SSLParameters sslParamters) throws IOException {
        this(handlerName, socketOptions, executor, 1);
    }

    /**
     * Close the open communication handlers managed by the receiver
     */
    public void closeOpenHandlers() {
        final Lock myLock = handlersLock;
        myLock.lock();
        try {
            SocketChannelHandler handler = openHandlers;
            while (handler != null) {
                handler.close();
                handler = handler.next();
            }
            openHandlers = null;
        } finally {
            myLock.unlock();
        }
    }

    /**
     * Connect to the remote address. The connection will be made in a
     * non-blocking fashion. The
     * CommunicationsHandler.handleConnect(SocketChannel) on the event handler
     * will be called when the socket channel actually connects.
     * 
     * @param remoteAddress
     * @param eventHandler
     * @throws IOException
     */
    public void connectTo(final InetSocketAddress remoteAddress,
                          CommunicationsHandler eventHandler)
                                                             throws IOException {
        assert remoteAddress != null : "Remote address cannot be null";
        assert eventHandler != null : "Handler cannot be null";
        int index = nextQueueIndex();
        SocketChannel socketChannel = SocketChannel.open();
        SocketChannelHandler handler = new SocketChannelHandler(
                                                                eventHandler,
                                                                ChannelHandler.this,
                                                                socketChannel,
                                                                index);
        options.configure(socketChannel.socket());
        addHandler(handler);
        socketChannel.configureBlocking(false);
        try {
            socketChannel.connect(remoteAddress);
        } catch (IOException e) {
            log.warn(String.format("Cannot connect to %s [%s]", remoteAddress,
                                   name), e);
            throw e;
        }
        registerConnect(index, socketChannel, handler);
        return;
    }

    public String getName() {
        return name;
    }

    /**
     * 
     * @return the list of open handlers the receiver manages
     */
    public List<CommunicationsHandler> getOpenHandlers() {
        LinkedList<CommunicationsHandler> handlers = new LinkedList<CommunicationsHandler>();
        final Lock myLock = handlersLock;
        myLock.lock();
        try {
            SocketChannelHandler current = openHandlers;
            while (current != null) {
                handlers.add(current.getHandler());
                current = current.next();
            }
        } finally {
            myLock.unlock();
        }

        return handlers;
    }

    /**
     * Answer the socket options of the receiver
     * 
     * @return
     */
    public SocketOptions getOptions() {
        return options;
    }

    /**
     * Answer true if the receiver is running
     * 
     * @return
     */
    public boolean isRunning() {
        return run.get();
    }

    /**
     * Starts the socket handler service
     */
    public void start() {
        if (run.compareAndSet(false, true)) {
            startService();
        }
    }

    /**
     * Terminates the socket handler service
     */
    public void terminate() {
        if (run.compareAndSet(true, false)) {
            terminateService();
        }
    }

    private void handleConnect(SocketChannelHandler handler,
                               SocketChannel channel) {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Handling connect on %s [%s]", channel,
                                    name));
        }
        try {
            channel.finishConnect();
        } catch (IOException e) {
            log.info(String.format("Unable to finish connection %s [%s] error: %s",
                                   handler.getChannel(), name, e));
            handler.close();
            return;
        }
        if (log.isTraceEnabled()) {
            log.trace(String.format("Dispatching connected action on %s [%s]",
                                    channel, name));
        }
        handler.handleConnect();
    }

    private void select(int index) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Selecting [%s]", name));
        }

        Runnable register = registers[index].pollFirst();
        while (register != null) {
            try {
                register.run();
            } catch (Throwable e) {
                log.error("Error when registering interests [{}]", name, e);
            }
            register = registers[index].pollFirst();
        }

        int ready = selectors[index].select(selectTimeout);

        if (log.isTraceEnabled()) {
            log.trace(String.format("Selected %s channels [%s]", ready, name));
        }

        // get an iterator over the set of selected keys
        Iterator<SelectionKey> selected;
        try {
            selected = selectors[index].selectedKeys().iterator();
        } catch (ClosedSelectorException e) {
            return;
        }

        while (run.get() && selected.hasNext()) {
            SelectionKey key = selected.next();
            selected.remove();
            SocketChannelHandler handler = (SocketChannelHandler) key.attachment();
            SocketChannel channel = (SocketChannel) key.channel();
            int interestOps = key.interestOps();
            if (key.isConnectable()) {
                key.interestOps(interestOps ^ SelectionKey.OP_CONNECT);
                handleConnect(handler, channel);
                continue;
            }

            if ((interestOps & SelectionKey.OP_READ) > 0 && key.isReadable()) {
                key.interestOps(interestOps ^ SelectionKey.OP_READ);
                handler.handleRead();
            }

            if ((interestOps & SelectionKey.OP_WRITE) > 0 && key.isWritable()) {
                key.interestOps(interestOps ^ SelectionKey.OP_WRITE);
                handler.handleWrite();
            }
        }
    }

    private Runnable selectorTask(final int index) {
        return new Runnable() {
            @Override
            public void run() {
                while (run.get()) {
                    try {
                        select(index);
                    } catch (ClosedSelectorException e) {
                        if (log.isTraceEnabled()) {
                            log.trace(String.format("Channel closed [%s]", name),
                                      e);
                        }
                        return;
                    } catch (IOException e) {
                        if (log.isTraceEnabled()) {
                            log.trace(String.format("Error when selecting [%s]",
                                                    name), e);
                        }
                        return;
                    } catch (Throwable e) {
                        log.error(String.format("Runtime exception when selecting [%s]",
                                                name), e);
                        return;
                    }
                }
            }
        };
    }

    void addHandler(SocketChannelHandler handler) {
        final Lock myLock = handlersLock;
        myLock.lock();
        try {
            if (openHandlers == null) {
                openHandlers = handler;
            } else {
                openHandlers.link(handler);
            }
        } finally {
            myLock.unlock();
        }
    }

    void delink(SocketChannelHandler handler) {
        final Lock myLock = handlersLock;
        myLock.lock();
        try {
            if (openHandlers == null) {
                openHandlers = handler.next();
            } else {
                handler.delink();
            }
        } finally {
            myLock.unlock();
        }
    }

    void execute(Runnable task) {
        executor.execute(task);
    }

    int nextQueueIndex() {
        return nextQueue.getAndIncrement() % selectors.length;
    }

    final void register(int index, Runnable select) {
        registers[index].add(select);
        wakeup(index);
    }

    SelectionKey register(int index, SocketChannel channel,
                          SocketChannelHandler handler, int operation) {
        assert !channel.isBlocking() : String.format("%s has not been set to non blocking mode [%s]",
                                                     channel, name);
        SelectionKey key = null;
        Selector selector = selectors[index];
        try {
            key = channel.keyFor(selector);
            if (key == null) {
                key = channel.register(selector, operation, handler);
            } else {
                key.interestOps(key.interestOps() | operation);
                key.attach(handler);
            }
        } catch (NullPointerException e) {
            // apparently the file descriptor can be nulled
            log.trace(String.format("anamalous null pointer exception [%s]",
                                    name), e);
        } catch (ClosedChannelException e) {
            if (log.isTraceEnabled()) {
                log.trace(String.format("%s has been closed [%s]", channel,
                                        name), e);
            }
            handler.close();
        }
        return key;
    }

    void registerConnect(final int index, final SocketChannel socketChannel,
                         final SocketChannelHandler handler) {
        registers[index].add(new Runnable() {
            @Override
            public void run() {
                if (log.isTraceEnabled()) {
                    log.trace(String.format("registering connect for %s [%s]",
                                            socketChannel, name));
                }
                register(index, socketChannel, handler, SelectionKey.OP_CONNECT);
            }
        });
        wakeup(index);
    }

    final Runnable selectForRead(final int index,
                                 final SocketChannelHandler handler) {
        return new Runnable() {
            @Override
            public void run() {
                if (log.isTraceEnabled()) {
                    log.trace(String.format("registering read for %s [%s]",
                                            handler.getChannel(), name));
                }
                register(index, handler.getConcreteChannel(), handler,
                         SelectionKey.OP_READ);
            }
        };
    }

    final Runnable selectForWrite(final int index,
                                  final SocketChannelHandler handler) {
        return new Runnable() {
            @Override
            public void run() {
                if (log.isTraceEnabled()) {
                    log.trace(String.format("registering write for %s [%s]",
                                            handler.getChannel(), name));
                }
                register(index, handler.getConcreteChannel(), handler,
                         SelectionKey.OP_WRITE);
            }
        };
    }

    void startService() {
        for (Thread selectorThread : selectorThreads) {
            selectorThread.start();
        }
        log.info(format("started [%s]", name));
    }

    void terminateService() {
        for (Selector selector : selectors) {
            selector.wakeup();
            try {
                selector.close();
            } catch (IOException e) {
                log.info(String.format("Error closing selector [%s]", name), e);
            }
        }
        closeOpenHandlers();
        log.info(format("terminated [%s]", name));
    }

    void wakeup(int index) {
        try {
            selectors[index].wakeup();
        } catch (NullPointerException e) {
            // Bug in JRE
            if (log.isTraceEnabled()) {
                log.trace(String.format("Caught null pointer in selector wakeup [%s]",
                                        name), e);
            }
        }
    }
}