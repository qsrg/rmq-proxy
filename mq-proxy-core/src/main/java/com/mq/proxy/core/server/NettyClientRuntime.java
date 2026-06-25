package com.mq.proxy.core.server;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.concurrent.ExecutorService;
import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyClientRuntime {

    private final NettyClientConfig nettyClientConfig;
    private final String threadNamePrefix;

    private volatile EventLoopGroup eventLoopGroup;
    private volatile ExecutorService callbackExecutor;
    private volatile ScheduledExecutorService responseTableScanExecutor;
    private volatile ScheduledFuture<?> responseTableScanFuture;
    private volatile boolean shutdown;
    private final Set<Runnable> responseTableScanners =
            Collections.newSetFromMap(new ConcurrentHashMap<Runnable, Boolean>());

    public NettyClientRuntime(NettyClientConfig nettyClientConfig, String threadNamePrefix) {
        this.nettyClientConfig = nettyClientConfig;
        this.threadNamePrefix = threadNamePrefix != null && threadNamePrefix.length() > 0
                ? threadNamePrefix
                : "NettyClient";
    }

    public synchronized void start() {
        if (this.shutdown) {
            throw new IllegalStateException("NettyClientRuntime already shutdown");
        }
        if (this.eventLoopGroup != null) {
            return;
        }

        this.eventLoopGroup = new NioEventLoopGroup(
                this.nettyClientConfig.getClientWorkerThreadNums(),
                newNamedThreadFactory(this.threadNamePrefix + "WorkerThread_"));
        int callbackThreads = this.nettyClientConfig.getClientCallbackExecutorThreads() > 0
                ? this.nettyClientConfig.getClientCallbackExecutorThreads()
                : 4;
        this.callbackExecutor = new ThreadPoolExecutor(
                callbackThreads,
                callbackThreads,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(10000),
                newNamedThreadFactory(this.threadNamePrefix + "CallbackThread_"));
        this.responseTableScanExecutor = new ScheduledThreadPoolExecutor(
                1,
                newNamedThreadFactory(this.threadNamePrefix + "ResponseScanThread_"));
        this.responseTableScanFuture = this.responseTableScanExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                for (Runnable scanner : responseTableScanners) {
                    scanner.run();
                }
            }
        }, 1000L, 1000L, TimeUnit.MILLISECONDS);
    }

    public ResponseTableScanRegistration scheduleResponseTableScan(Runnable scanner) {
        if (this.shutdown) {
            throw new IllegalStateException("NettyClientRuntime already shutdown");
        }
        if (this.responseTableScanFuture == null) {
            throw new IllegalStateException("NettyClientRuntime is not started");
        }
        this.responseTableScanners.add(scanner);
        return new ResponseTableScanRegistration(scanner, this.responseTableScanFuture);
    }

    public synchronized void shutdown() {
        this.shutdown = true;
        if (this.responseTableScanExecutor != null) {
            this.responseTableScanExecutor.shutdown();
        }
        this.responseTableScanners.clear();
        if (this.callbackExecutor != null) {
            this.callbackExecutor.shutdown();
        }
        if (this.eventLoopGroup != null) {
            this.eventLoopGroup.shutdownGracefully();
        }
    }

    public boolean isShutdown() {
        return this.shutdown;
    }

    public EventLoopGroup getEventLoopGroup() {
        return eventLoopGroup;
    }

    public ExecutorService getCallbackExecutor() {
        return callbackExecutor;
    }

    public int getCallbackExecutorActiveCount() {
        ExecutorService executor = this.callbackExecutor;
        if (executor instanceof ThreadPoolExecutor) {
            return ((ThreadPoolExecutor) executor).getActiveCount();
        }
        return 0;
    }

    public int getCallbackExecutorQueueSize() {
        ExecutorService executor = this.callbackExecutor;
        if (executor instanceof ThreadPoolExecutor) {
            return ((ThreadPoolExecutor) executor).getQueue().size();
        }
        return 0;
    }

    public ScheduledExecutorService getResponseTableScanExecutor() {
        return responseTableScanExecutor;
    }

    ScheduledFuture<?> getResponseTableScanFuture() {
        return responseTableScanFuture;
    }

    public class ResponseTableScanRegistration {
        private final Runnable scanner;
        private final ScheduledFuture<?> sharedFuture;

        private ResponseTableScanRegistration(Runnable scanner, ScheduledFuture<?> sharedFuture) {
            this.scanner = scanner;
            this.sharedFuture = sharedFuture;
        }

        public void cancel() {
            responseTableScanners.remove(scanner);
        }

        ScheduledFuture<?> getSharedFuture() {
            return sharedFuture;
        }
    }

    private ThreadFactory newNamedThreadFactory(final String prefix) {
        return new ThreadFactory() {
            private final AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, prefix + threadIndex.incrementAndGet());
            }
        };
    }
}
