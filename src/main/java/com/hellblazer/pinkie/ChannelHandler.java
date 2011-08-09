/** (C) Copyright 2011 Hal Hildebrand, all rights reserved.

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package com.hellblazer.pinkie;

import static java.lang.String.format;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
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
 * This class provides a full featured non blocking NIO channel handler with
 * outbound connection capabilities.
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public abstract class ChannelHandler {
    private final static Logger           log           = Logger.getLogger(ChannelHandler.class.getCanonicalName());

    final Executor                        commsExecutor;
    final SocketOptions                   options;
    private final ReentrantLock           handlersLock  = new ReentrantLock();
    private final InetSocketAddress       localAddress;
    private final String                  name;
    private volatile SocketChannelHandler openHandlers;
    private final AtomicBoolean           run           = new AtomicBoolean();
    private final Selector                selector;
    private final ExecutorService         selectService;
    private Future<?>                     selectTask;
    private final int                     selectTimeout = 1000;
    private final SelectableChannel       server;

    /**
     * Construct a new channel handler
     * 
     * @param handlerName
     *            - the String name used to mark the selection thread
     * @param channel
     *            - the selectable channel to handle
     * @param endpointAddress
     *            - the local endpoint address of the handler
     * @param socketOptions
     *            - the socket options to configure accpeted sockets
     * @param commsExec
     *            - the executor service to handle I/O events
     * @throws IOException
     *             - if things go pear shaped
     */
    public ChannelHandler(String handlerName, SelectableChannel channel,
                          InetSocketAddress endpointAddress,
                          SocketOptions socketOptions, Executor commsExec)
                                                                          throws IOException {
        name = handlerName;
        server = channel;
        localAddress = endpointAddress;
        commsExecutor = commsExec;
        options = socketOptions;
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
        server.configureBlocking(false);
        server.register(selector, SelectionKey.OP_ACCEPT);
    }

    /**
     * Answer the local address of the endpoint
     * 
     * @return
     */
    public InetSocketAddress getLocalAddress() {
        return localAddress;
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
            log.info(format("%s is started, local address: %s", name,
                            localAddress));
        }
    }

    /**
     * Terminates the socket handler service
     */
    public void terminate() {
        if (run.compareAndSet(true, false)) {
            terminateService();
            log.info(format("%s is terminated, local address: %s", name,
                            localAddress));
        }
    }

    /**
     * Create an instance of a socket channel handler, using the supplied
     * channel and key.
     * 
     * @param channel
     * @return
     */
    abstract protected SocketChannelHandler createHandler(SocketChannel channel);

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
        if (key.isAcceptable()) {
            handleAccept(key);
        }
        if (key.isReadable()) {
            handleRead(key);
        }
        if (key.isWritable()) {
            handleWrite(key);
        }
    }

    void handleAccept(SelectionKey key) throws IOException {
        if (log.isLoggable(Level.FINEST)) {
            log.finest("Handling accept");
        }
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel accepted = server.accept();
        options.configure(accepted.socket());
        accepted.configureBlocking(false);
        if (log.isLoggable(Level.FINE)) {
            log.fine(String.format("Connection accepted: %s", accepted));
        }
        SocketChannelHandler handler = createHandler(accepted);
        SelectionKey newKey = register(accepted, handler, 0);
        addHandler(handler);
        newKey.attach(handler);
        try {
            commsExecutor.execute(handler.acceptHandler());
        } catch (RejectedExecutionException e) {
            if (log.isLoggable(Level.INFO)) {
                log.log(Level.INFO, "too busy to execute accept handling");
            }
        }
    }

    void handleRead(SelectionKey key) {
        if (log.isLoggable(Level.FINEST)) {
            log.finest("Handling read");
        }
        key.interestOps(key.interestOps() & (~SelectionKey.OP_READ));
        try {
            commsExecutor.execute(((SocketChannelHandler) key.attachment()).readHandler);
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
        key.interestOps(key.interestOps() & (~SelectionKey.OP_WRITE));
        try {
            commsExecutor.execute(((SocketChannelHandler) key.attachment()).writeHandler);
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

    void selectForRead(SocketChannelHandler handler) {
        SelectionKey key = handler.getChannel().keyFor(selector);
        key.interestOps(key.interestOps() | SelectionKey.OP_READ);
        wakeup();
    }

    void selectForWrite(SocketChannelHandler handler) {
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
                            log.log(Level.FINE, "Error when selecting: "
                                                + server, e);
                        }
                    } catch (CancelledKeyException e) {
                        if (log.isLoggable(Level.FINE)) {
                            log.log(Level.FINE, "Error when selecting: "
                                                + server, e);
                        }
                    } catch (Throwable e) {
                        log.log(Level.SEVERE,
                                "Runtime exception when selecting", e);
                    }
                }
            }
        });
    }

    void terminateService() {
        selector.wakeup();
        try {
            server.close();
        } catch (IOException e) {
            // do not log
        }
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
            SocketChannelHandler handler = openHandlers;
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