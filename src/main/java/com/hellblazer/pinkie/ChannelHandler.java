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
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class provides a full featured non blocking NIO handler for selectable
 * channels. In addition, it provides facilities for dealing with outbound
 * connections.
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class ChannelHandler<T extends CommunicationsHandler> {
    private final static Logger              log           = Logger.getLogger(ChannelHandler.class.getCanonicalName());

    private final ReentrantLock              handlersLock  = new ReentrantLock();
    private volatile SocketChannelHandler<T> openHandlers;
    private final AtomicBoolean              run           = new AtomicBoolean();
    private final ExecutorService            selectService;
    private Future<?>                        selectTask;
    private final int                        selectTimeout = 1000;
    protected final String                   name;
    protected final Selector                 selector;
    final Executor                           commsExecutor;
    final SocketOptions                      options;

    /**
     * Construct a new channel handler
     * 
     * @param handlerName
     *            - the String name used to mark the selection thread
     * @param socketOptions
     *            - the socket options to configure new sockets
     * @param commsExec
     *            - the executor service to handle I/O events
     * @throws IOException
     *             - if things go pear shaped when opening the selector
     */
    public ChannelHandler(String handlerName,
                                    SocketOptions socketOptions,
                                    Executor commsExec) throws IOException {
        name = handlerName;
        selectService = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread daemon = new Thread(
                                           r,
                                           format("Server channel handler select for %s",
                                                  name));
                daemon.setDaemon(true);
                daemon.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        log.log(Level.WARNING,
                                "Uncaught exception on select handler", e);
                    }
                });
                return daemon;
            }
        });
        selector = Selector.open();
        commsExecutor = commsExec;
        options = socketOptions;
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
    public void connectTo(InetSocketAddress remoteAddress, T eventHandler)
                                                                          throws IOException {
        SocketChannel socketChannel = SocketChannel.open();
        options.configure(socketChannel.socket());
        SocketChannelHandler<T> handler = new SocketChannelHandler<T>(
                                                                      eventHandler,
                                                                      this,
                                                                      socketChannel);
        addHandler(handler);
        SelectionKey key = register(socketChannel, handler, 0);
        socketChannel.configureBlocking(false);
        if (socketChannel.connect(remoteAddress)) {
            try {
                commsExecutor.execute(handler.connectHandler());
            } catch (RejectedExecutionException e) {
                if (log.isLoggable(Level.INFO)) {
                    log.log(Level.INFO,
                            "too busy to execute connection handling");
                }
            }
            return;
        }
        key.interestOps(key.interestOps() | SelectionKey.OP_CONNECT);
        wakeup();
        return;
    }

    public List<T> getOpenHandlers() {
        LinkedList<T> handlers = new LinkedList<T>();
        final Lock myLock = handlersLock;
        myLock.lock();
        try {
            SocketChannelHandler<T> current = openHandlers;
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

    void addHandler(SocketChannelHandler<T> handler) {
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

    void closeHandler(SocketChannelHandler<T> handler) {
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

    void dispatch(SelectionKey key) throws IOException {
        if (key.isConnectable()) {
            handleConnect(key);
        } else {
            if (key.isReadable()) {
                handleRead(key);
            }
            if (key.isWritable()) {
                handleWrite(key);
            }
        }
    }

    void handleConnect(SelectionKey key) {
        if (log.isLoggable(Level.FINEST)) {
            log.finest("Handling read");
        }
        key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT);
        try {
            ((SocketChannel) key.channel()).finishConnect();
        } catch (IOException e) {
            log.log(Level.SEVERE, "Unable to finish connection", e);
        }
        if (log.isLoggable(Level.FINE)) {
            log.fine("Dispatching connected action");
        }
        try {
            @SuppressWarnings("unchecked")
            SocketChannelHandler<T> handler = (SocketChannelHandler<T>) key.attachment();
            commsExecutor.execute(handler.connectHandler());
        } catch (RejectedExecutionException e) {
            if (log.isLoggable(Level.FINEST)) {
                log.log(Level.FINEST, "cannot execute connect action", e);
            }
        }
    }

    void handleRead(SelectionKey key) {
        if (log.isLoggable(Level.FINEST)) {
            log.finest("Handling read");
        }
        key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
        try {
            @SuppressWarnings("unchecked")
            SocketChannelHandler<T> handler = (SocketChannelHandler<T>) key.attachment();
            commsExecutor.execute(handler.readHandler);
        } catch (RejectedExecutionException e) {
            if (log.isLoggable(Level.INFO)) {
                log.log(Level.INFO, "too busy to execute read handling");
            }
        }
    }

    void handleWrite(SelectionKey key) {
        if (log.isLoggable(Level.FINEST)) {
            log.finest("Handling write");
        }
        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        try {
            @SuppressWarnings("unchecked")
            SocketChannelHandler<T> handler = (SocketChannelHandler<T>) key.attachment();
            commsExecutor.execute(handler.writeHandler);
        } catch (RejectedExecutionException e) {
            if (log.isLoggable(Level.INFO)) {
                log.log(Level.INFO, "too busy to execute write handling");
            }
        }
    }

    SelectionKey register(SocketChannel channel, Object handler, int operation) {
        SelectionKey key = null;
        try {
            key = channel.register(selector, operation, handler);
        } catch (NullPointerException e) {
            // apparently the file descriptor can be nulled
            log.log(Level.FINEST, "anamalous null pointer exception", e);
        } catch (ClosedChannelException e) {
            if (log.isLoggable(Level.FINEST)) {
                log.log(Level.FINEST, "channel has been closed", e);
            }
        }
        return key;
    }

    void select() throws IOException {
        if (log.isLoggable(Level.FINEST)) {
            log.finest("Selecting");
        }
        selector.select(selectTimeout);

        // get an iterator over the set of selected keys
        Iterator<SelectionKey> selected;
        try {
            selected = selector.selectedKeys().iterator();
        } catch (ClosedSelectorException e) {
            return;
        }

        while (run.get() && selected.hasNext()) {
            SelectionKey key = selected.next();
            selected.remove();
            try {
                dispatch(key);
            } catch (CancelledKeyException e) {
                if (log.isLoggable(Level.FINE)) {
                    log.log(Level.FINE, format("Cancelled Key: %s", key), e);
                }
            }
        }
    }

    void selectForRead(SocketChannelHandler<T> handler) {
        SelectionKey key = handler.getChannel().keyFor(selector);
        key.interestOps(key.interestOps() | SelectionKey.OP_READ);
        wakeup();
    }

    void selectForWrite(SocketChannelHandler<T> handler) {
        SelectionKey key = handler.getChannel().keyFor(selector);
        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        wakeup();
    }

    void startService() {
        selectTask = selectService.submit(new Runnable() {
            @Override
            public void run() {
                while (run.get()) {
                    try {
                        select();
                    } catch (ClosedSelectorException e) {
                        if (log.isLoggable(Level.FINE)) {
                            log.log(Level.FINER, "Channel closed", e);
                        }
                    } catch (IOException e) {
                        if (log.isLoggable(Level.FINE)) {
                            log.log(Level.FINE, "Error when selecting", e);
                        }
                    } catch (CancelledKeyException e) {
                        if (log.isLoggable(Level.FINE)) {
                            log.log(Level.FINE, "Error when selecting", e);
                        }
                    } catch (Throwable e) {
                        log.log(Level.SEVERE,
                                "Runtime exception when selecting", e);
                    }
                }
            }
        });
        log.info(format("%s is started", name));
    }

    void terminateService() {
        selector.wakeup();
        try {
            selector.close();
        } catch (IOException e) {
            // do not log
        }
        selectTask.cancel(true);
        selectService.shutdownNow();
        final Lock myLock = handlersLock;
        myLock.lock();
        try {
            SocketChannelHandler<T> handler = openHandlers;
            while (handler != null) {
                handler.internalClose();
            }
            openHandlers = null;
        } finally {
            myLock.unlock();
        }
    }

    void wakeup() {
        try {
            selector.wakeup();
        } catch (NullPointerException e) {
            // Bug in JRE
            if (log.isLoggable(Level.FINEST)) {
                log.log(Level.FINEST, "Caught null pointer in selector wakeup",
                        e);
            }
        }
    }
}