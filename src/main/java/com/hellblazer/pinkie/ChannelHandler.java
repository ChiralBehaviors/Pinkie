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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
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
public class ChannelHandler {
    private final static Logger                   log           = Logger.getLogger(ChannelHandler.class.getCanonicalName());

    private final ReentrantLock                   handlersLock  = new ReentrantLock();
    private volatile SocketChannelHandler         openHandlers;
    private final AtomicBoolean                   run           = new AtomicBoolean();
    private Future<?>                             selectTask;
    private final int                             selectTimeout = 1000;
    protected final ExecutorService               executor;
    protected final String                        name;
    protected final SocketOptions                 options;
    protected final LinkedBlockingDeque<Runnable> registers     = new LinkedBlockingDeque<Runnable>();
    protected final Selector                      selector;

    /**
     * Construct a new channel handler
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
        name = handlerName;
        selector = Selector.open();
        this.executor = executor;
        options = socketOptions;
    }

    /**
     * Close the open handlers managed by the receiver
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
        final SocketChannel socketChannel = SocketChannel.open();
        final SocketChannelHandler handler = new SocketChannelHandler(
                                                                      eventHandler,
                                                                      ChannelHandler.this,
                                                                      socketChannel);
        options.configure(socketChannel.socket());
        addHandler(handler);
        socketChannel.configureBlocking(false);
        registers.add(new Runnable() {
            @Override
            public void run() {
                register(socketChannel, handler, SelectionKey.OP_CONNECT);
                try {
                    socketChannel.connect(remoteAddress);
                } catch (IOException e) {
                    log.log(Level.WARNING,
                            String.format("Cannot connect to %s [%s]",
                                          remoteAddress, name), e);
                    handler.close();
                    return;
                }
            }
        });
        wakeup();
        return;
    }

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

    void closeHandler(SocketChannelHandler handler) {
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
        key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT);
        if (log.isLoggable(Level.FINEST)) {
            log.finest(String.format("Handling connect [%s]", name));
        }
        SocketChannelHandler handler = (SocketChannelHandler) key.attachment();
        try {
            ((SocketChannel) key.channel()).finishConnect();
        } catch (IOException e) {
            log.log(Level.INFO,
                    String.format("Unable to finish connection %s [%s] error: %s",
                                  handler.getChannel(), name, e));
            handler.close();
            return;
        }
        if (log.isLoggable(Level.FINE)) {
            log.fine(String.format("Dispatching connected action [%s]", name));
        }
        try {
            executor.execute(handler.connectHandler());
        } catch (RejectedExecutionException e) {
            if (log.isLoggable(Level.INFO)) {
                log.log(Level.INFO,
                        String.format("too busy to execute connect handling [%s] of [%s]",
                                      name, handler.getChannel()));
            }
            handler.close();
        }
    }

    void handleRead(SelectionKey key) {
        key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
        if (log.isLoggable(Level.FINEST)) {
            log.finest(String.format("Handling read [%s]", name));
        }
        SocketChannelHandler handler = (SocketChannelHandler) key.attachment();
        try {
            executor.execute(handler.readHandler);
        } catch (RejectedExecutionException e) {
            if (log.isLoggable(Level.INFO)) {
                log.log(Level.INFO,
                        String.format("too busy to execute read handling [%s], reselecting [%s]",
                                      name, handler.getChannel()));
            }
            handler.selectForRead();
        }
    }

    void handleWrite(SelectionKey key) {
        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        if (log.isLoggable(Level.FINEST)) {
            log.finest(String.format("Handling write [%s]", name));
        }
        SocketChannelHandler handler = (SocketChannelHandler) key.attachment();
        try {
            executor.execute(handler.writeHandler);
        } catch (RejectedExecutionException e) {
            if (log.isLoggable(Level.INFO)) {
                log.log(Level.INFO,
                        String.format("too busy to execute write handling [%s], reselecting [%s]",
                                      name, handler.getChannel()));
            }
            handler.selectForWrite();
        }
    }

    final void register(Runnable select) {
        registers.add(select);
        wakeup();
    }

    SelectionKey register(SocketChannel channel, SocketChannelHandler handler,
                          int operation) {
        assert !channel.isBlocking() : String.format("Socket has not been set to non blocking mode [%s]",
                                                     name);
        SelectionKey key = null;
        try {
            key = channel.register(selector, operation, handler);
        } catch (NullPointerException e) {
            // apparently the file descriptor can be nulled
            log.log(Level.FINEST,
                    String.format("anamalous null pointer exception [%s]", name),
                    e);
        } catch (ClosedChannelException e) {
            if (log.isLoggable(Level.FINEST)) {
                log.log(Level.FINEST,
                        String.format("channel has been closed [%s]", name), e);
            }
            handler.close();
        }
        return key;
    }

    void select() throws IOException {
        if (log.isLoggable(Level.FINEST)) {
            log.finest(String.format("Selecting [%s]", name));
        }

        Runnable register = registers.pollFirst();
        while (register != null) {
            register.run();
            register = registers.pollFirst();
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
                if (log.isLoggable(Level.FINEST)) {
                    log.log(Level.FINEST,
                            format("Cancelled Key: %s [%s]", key, name), e);
                }
            }
        }
    }

    final Runnable selectForRead(final SocketChannelHandler handler) {
        return new Runnable() {
            @Override
            public void run() {
                SelectionKey key = handler.getChannel().keyFor(selector);
                if (key == null) {
                    log.finest(String.format("Key is null for %s [%s]",
                                             handler.getChannel(), name));
                    return;
                }
                key.interestOps(key.interestOps() | SelectionKey.OP_READ);
            }
        };
    }

    final Runnable selectForWrite(final SocketChannelHandler handler) {
        return new Runnable() {
            @Override
            public void run() {
                SelectionKey key = handler.getChannel().keyFor(selector);
                if (key == null) {
                    log.finest(String.format("Key is null for %s [%s]",
                                             handler.getChannel(), name));
                    return;
                }
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            }
        };
    }

    void startService() {
        selectTask = executor.submit(new Runnable() {
            @Override
            public void run() {
                while (run.get()) {
                    try {
                        select();
                    } catch (ClosedSelectorException e) {
                        if (log.isLoggable(Level.FINE)) {
                            log.log(Level.FINER,
                                    String.format("Channel closed [%s]", name),
                                    e);
                        }
                    } catch (IOException e) {
                        if (log.isLoggable(Level.FINE)) {
                            log.log(Level.FINE,
                                    String.format("Error when selecting [%s]",
                                                  name), e);
                        }
                    } catch (CancelledKeyException e) {
                        if (log.isLoggable(Level.FINE)) {
                            log.log(Level.FINE,
                                    String.format("Error when selecting [%s]",
                                                  name), e);
                        }
                    } catch (Throwable e) {
                        log.log(Level.SEVERE,
                                String.format("Runtime exception when selecting [%s]",
                                              name), e);
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
            log.log(Level.INFO,
                    String.format("Error closing selector [%s]", name), e);
        }
        selectTask.cancel(true);
        closeOpenHandlers();
        log.info(format("%s is terminated", name));
    }

    final void wakeup() {
        try {
            selector.wakeup();
        } catch (NullPointerException e) {
            // Bug in JRE
            if (log.isLoggable(Level.FINEST)) {
                log.log(Level.FINEST,
                        String.format("Caught null pointer in selector wakeup [%s]",
                                      name), e);
            }
        }
    }
}