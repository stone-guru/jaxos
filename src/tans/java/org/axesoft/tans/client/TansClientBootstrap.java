package org.axesoft.tans.client;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.apache.commons.lang3.tuple.Pair;
import org.axesoft.jaxos.base.Either;
import org.axesoft.jaxos.base.LongRange;
import org.axesoft.tans.server.RedirectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.BufferOverflowException;
import java.util.List;
import java.util.concurrent.*;


/**
 * @author gaoyuan
 * @sine 2019/9/29.
 */
public class TansClientBootstrap {
    private final static int MAX_CONNECTION_COUNT = 20;
    private final static int MAX_CONNECTION_IDLE_SECONDS = 30;

    private final static AttributeKey<InetSocketAddress> ATTR_ADDRESS = AttributeKey.newInstance("ADDRESS");
    private final static AttributeKey<HttpClient> HTTP_CLIENT = AttributeKey.newInstance("HTTP_CLIENT");
    private final static AttributeKey<Long> USED_TIMESTAMP = AttributeKey.newInstance("USED_TIMESTAMP");
    private final static AttributeKey<CountDownLatch> LATCH = AttributeKey.newInstance("LATCH");
    private final static AttributeKey<LongRange> RESULT = AttributeKey.newInstance("RESULT");

    private final static String TANS_HANDLER_NAME = "tansHandler";

    private static final Logger logger = LoggerFactory.getLogger(TansClientBootstrap.class);

    private class HttpClient {
        private final String host;
        private final int port;

        private Bootstrap bootstrap;
        private ChannelPool channelPool;

        private HttpClient(NioEventLoopGroup worker, String host, int port) {
            this.host = host;
            this.port = port;
            this.bootstrap = new Bootstrap();
            this.bootstrap.group(worker)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .remoteAddress(host, port);

            this.channelPool = new SimpleChannelPool(bootstrap, new ChannelPoolHandler() {
                @Override
                public void channelReleased(Channel ch) throws Exception {
                    logger.info("Channel released {}", ch.remoteAddress());
                }

                @Override
                public void channelAcquired(Channel ch) throws Exception {
                    logger.info("Channel acquired {}", ch.remoteAddress());
                }

                @Override
                public void channelCreated(Channel ch) throws Exception {
                    logger.info("Channel created {}", ch.remoteAddress());
                    ChannelPipeline p = ch.pipeline();
                    p.addLast(new HttpClientCodec());
                    //p.addLast(new HttpObjectAggregator(1048576));
                    p.addLast(TANS_HANDLER_NAME, new TansClientHandler());
                    ch.attr(HTTP_CLIENT).set(HttpClient.this);
                }
            });
        }

        private Channel getChannel() {
            try {
                return this.channelPool.acquire().get();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            catch (ExecutionException e) {
                e.printStackTrace();
            }
            return null;
        }

        private void returnChannel(Channel channel) {
            this.channelPool.release(channel);
        }
    }

    private class TansClientHandler extends SimpleChannelInboundHandler<HttpObject> {
        private BlockingQueue<Promise<Either<String, LongRange>>> promises = new ArrayBlockingQueue<>(8);
        private ChannelHandlerContext ctx;
        private HttpResponse response;

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            super.channelActive(ctx);
            this.ctx = ctx;
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);

            synchronized (this) {
                Promise<Either<String, LongRange>> promise;
                while ((promise = this.promises.poll()) != null) {
                    promise.setFailure(new IOException("Connection lost"));
                }
                this.promises = null;
                Channel channel = ctx.channel();
                HttpClient client = channel.attr(HTTP_CLIENT).get();
                logger.info("Channel to {}:{} closed", client.host, client.port);
            }
        }


        @Override
        public synchronized void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
            if (this.promises == null) {
                logger.info("Handler closed, abandon response");
                return;
            }

            //System.out.println("read a message");
            if (msg instanceof HttpResponse) {
                response = (HttpResponse) msg;
                //System.err.println("STATUS: " + response.status());
            }

            if (msg instanceof HttpContent) {
                HttpContent content = (HttpContent) msg;
                Channel channel = ctx.channel();
                String body = content.content().toString(CharsetUtil.UTF_8);

                if (response.status().code() == 200) {
                    LongRange r;
                    try {
                        String s = firstLine(body);
                        String[] rx = s.split(",");
                        r = new LongRange(Long.parseLong(rx[1]), Long.parseLong(rx[2]));
                    }
                    catch (Exception e) {
                        promises.poll().setFailure(e);
                        return;
                    }
                    channel.attr(RESULT).set(r);
                    promises.poll().setSuccess(Either.right(r));
                }
                else if (isRedirectCode(response.status().code())) {
                    promises.poll().setFailure(new UnsupportedOperationException("Redirect"));
                }
                else {
                    promises.poll().setFailure(new Exception(body));
                }
//
//                InetSocketAddress addr = channel.attr(ATTR_ADDRESS).get();
//
//                HttpClient client = channel.attr(HTTP_CLIENT).get();
//
//                boolean isRedirect = isRedirectCode(response.status().code());
//
//                String s = isRedirect ?
//                        response.headers().get(HttpHeaderNames.LOCATION)
//                        : content.content().toString(CharsetUtil.UTF_8).lines().findFirst().orElseGet(() -> "");

//                String info = String.format("%s, %s, %s [%s]",
//                        response.headers().get(HttpHeaderNames.HOST),
//                        response.headers().get(HttpHeaderNames.FROM),
//                        response.status().codeAsText(),
//                        s);
//
//                logger.info("got result {}", info);
                //channel.attr(LATCH).get().countDown();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }


        public synchronized io.netty.util.concurrent.Future<Either<String, LongRange>> acquire(String key, int n) {
            if (this.promises == null) {
                return this.ctx.executor().newFailedFuture(new IllegalStateException("Channel closed"));
            }

            HttpRequest request;
            try {
                request = requestCache.get(Pair.of(key, n));
            }
            catch (ExecutionException e) {
                return this.ctx.executor().newFailedFuture(e);
            }

            Promise<Either<String, LongRange>> p = this.ctx.executor().newPromise();
            if (promises.offer(p)) {
                ctx.writeAndFlush(request);
            }
            else {
                p.setFailure(new BufferOverflowException());
            }

            return p;
        }
    }

    private class PooledTansClient implements TansClient {
        private HttpClient httpClient;
        private Channel channel;

        public PooledTansClient(HttpClient httpClient) {
            this.httpClient = httpClient;
            this.channel = httpClient.getChannel();
        }

        @Override
        public Future<LongRange> acquire(String key, int n) {
            final Promise<LongRange> promise = new DefaultPromise<>(channel.eventLoop());

            TansClientHandler handler = (TansClientHandler) this.channel.pipeline().get(TANS_HANDLER_NAME);
            handler.acquire(key, n).addListener(f -> {
                if (f.isSuccess()) {
                    @SuppressWarnings("unchecked")
                    Either<String, LongRange> r = ((Future<Either<String, LongRange>>) f).get();
                    if (r.isRight()) {
                        promise.setSuccess(r.getRight());
                    }
                    else {
                        promise.setFailure(new RedirectException(0));
                    }
                }
                else {
                    promise.setFailure(f.cause());
                }
            });

            return promise;
        }

        @Override
        public void close() {
            synchronized (this) {
                this.httpClient.returnChannel(this.channel);
                this.httpClient = null;
                this.channel = null;
            }
        }
    }

    private ConcurrentMap<InetSocketAddress, HttpClient> clientMap;
    private NioEventLoopGroup worker;

    private LoadingCache<Pair<String, Integer>, HttpRequest> requestCache;

    public TansClientBootstrap(String servers) {
        worker = new NioEventLoopGroup();

        clientMap = new ConcurrentHashMap<>();
        for (InetSocketAddress addr : parseAddresses(servers)) {
            getOrCreateHttpClient(addr);
        }

        requestCache = CacheBuilder.newBuilder()
                .concurrencyLevel(16)
                .expireAfterAccess(30, TimeUnit.SECONDS)
                .build(new CacheLoader<Pair<String, Integer>, HttpRequest>() {
                    @Override
                    public HttpRequest load(Pair<String, Integer> req) throws Exception {
                        HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                                String.format("/acquire?key=%s&n=%d", req.getKey(), req.getValue()), Unpooled.EMPTY_BUFFER);
                        request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
                        request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.TEXT_PLAIN);

                        return request;
                    }
                });
    }

    public TansClient getClient() {
        HttpClient client = selectServer("");
        return new PooledTansClient(client);
    }

    public void close() {
        this.worker.shutdownGracefully();
    }

    private HttpClient selectServer(String key) {
        return this.clientMap.values().iterator().next();
    }

    private List<InetSocketAddress> parseAddresses(String servers) {
        ImmutableList.Builder<InetSocketAddress> builder = ImmutableList.builder();
        String[] sx = servers.split(";");
        for (String s : sx) {
            String[] ax = s.split(":");
            String host = ax[0];
            int port = Integer.parseInt(ax[1]);
            builder.add(InetSocketAddress.createUnresolved(host, port));
        }
        return builder.build();
    }

    private HttpClient getOrCreateHttpClient(InetSocketAddress address) {
        return clientMap.compute(address, (k, c) -> {
            if (c != null) {
                return c;
            }
            return new HttpClient(this.worker, k.getHostName(), k.getPort());
        });
    }


    public static boolean isRedirectCode(int code) {
        switch (code) {
            case 300:
            case 301:
            case 302:
            case 303:
            case 305:
            case 307:
                return true;
            default:
                return false;
        }
    }

    public static String firstLine(String s) {
        int index = s.indexOf('\r');
        if (index < 0) {
            return s;
        }
        return s.substring(0, index);
    }
}
