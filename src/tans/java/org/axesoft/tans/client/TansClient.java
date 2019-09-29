package org.axesoft.tans.client;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.axesoft.jaxos.base.LongRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author gaoyuan
 * @sine 2019/9/29.
 */
public class TansClient {
    private final static int MAX_CONNECTION_COUNT = 20;
    private final static int MAX_CONNECTION_IDLE_SECONDS = 30;

    private final static AttributeKey<InetSocketAddress> ATTR_ADDRESS = AttributeKey.newInstance("ADDRESS");
    private final static AttributeKey<HttpClient> HTTP_CLIENT = AttributeKey.newInstance("HTTP_CLIENT");
    private final static AttributeKey<Long> USED_TIMESTAMP = AttributeKey.newInstance("USED_TIMESTAMP");
    private final static AttributeKey<CountDownLatch> LATCH = AttributeKey.newInstance("LATCH");
    private final static AttributeKey<LongRange> RESULT = AttributeKey.newInstance("RESULT");

    private static final Logger logger = LoggerFactory.getLogger(TansClient.class);

    private class HttpClient {
        private final String host;
        private final int port;

        private Bootstrap bootstrap;
        private BlockingDeque<Channel> channelQueue;
        private AtomicInteger connectionCount = new AtomicInteger(0);
        private AtomicInteger processedRequestCount = new AtomicInteger(0);

        private HttpClient(NioEventLoopGroup worker, String host, int port) {
            this.host = host;
            this.port = port;
            this.channelQueue = new LinkedBlockingDeque<>(MAX_CONNECTION_COUNT);
            this.bootstrap = new Bootstrap();
            this.bootstrap.group(worker)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new HttpClientCodec());
                            // Remove the following line if you don't want automatic content decompression.
                            p.addLast(new HttpContentDecompressor());
                            // Uncomment the following line if you don't want to handle HttpContents.
                            //p.addLast(new HttpObjectAggregator(1048576));
                            p.addLast(new HttpClientHandler());
                        }
                    });
        }

        private void createOneConnection() {
            ChannelFuture future = this.bootstrap.connect(host, port);
            future.addListener(f -> {
                if (f.isSuccess()) {
                    logger.info("connected to {}:{}", host, port);
                    Channel channel = ((ChannelFuture) f).channel();
                    channel.attr(ATTR_ADDRESS).set(InetSocketAddress.createUnresolved(host, port));
                    channel.attr(HTTP_CLIENT).set(this);
                    channelQueue.addLast(((ChannelFuture) f).channel());
                    connectionCount.incrementAndGet();
                }
                else {
                    logger.error("failed to connect to {}:{}", host, port);
                }
            });
        }

        private Channel getChannel() throws TimeoutException {
            long t0 = System.currentTimeMillis();
            Channel channel;
            do {
                try {
                    channel = channelQueue.poll(1, TimeUnit.MILLISECONDS);
                    if (channel == null || !channel.isActive()) {
                        if (System.currentTimeMillis() - t0 >= 1000) {
                            throw new TimeoutException();
                        }
                        if (connectionCount.get() < MAX_CONNECTION_COUNT) {
                            createOneConnection();
                        }
                        channel = null;
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            } while (channel == null);

            return channel;
        }

        private void returnChannel(Channel channel) {
            channelQueue.offerLast(channel);
        }
    }

    private class HttpClientHandler extends SimpleChannelInboundHandler<HttpObject> {
        HttpResponse response;

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);
            Channel channel = ctx.channel();
            HttpClient client = channel.attr(HTTP_CLIENT).get();
            client.channelQueue.remove(channel);
            int n = client.connectionCount.decrementAndGet();

            logger.info("Channel to {}:{} closed, {} remain", client.host, client.port, n);
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
            //System.out.println("read a message");
            if (msg instanceof HttpResponse) {
                response = (HttpResponse) msg;
                //System.err.println("STATUS: " + response.status());
            }
            if (msg instanceof HttpContent) {
                HttpContent content = (HttpContent) msg;
                Channel channel = ctx.channel();

                InetSocketAddress addr = channel.attr(ATTR_ADDRESS).get();

                HttpClient client = channel.attr(HTTP_CLIENT).get();
                client.processedRequestCount.incrementAndGet();

                boolean isRedirect = isRedirectCode(response.status().code());

                String s = isRedirect ?
                        response.headers().get(HttpHeaderNames.LOCATION)
                        : content.content().toString(CharsetUtil.UTF_8).lines().findFirst().orElseGet(() -> "");

                String info = String.format("%s, %s, %s [%s]",
                        response.headers().get(HttpHeaderNames.HOST),
                        response.headers().get(HttpHeaderNames.FROM),
                        response.status().codeAsText(),
                        s);

                //logger.info("got result {}", info);

                String[] rx = s.split(" *, *");
                LongRange r = new LongRange(Long.parseLong(rx[1]), Long.parseLong(rx[2]));
                channel.attr(RESULT).set(r);

                channel.attr(LATCH).get().countDown();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
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

    private ConcurrentMap<InetSocketAddress, HttpClient> clientMap;
    private NioEventLoopGroup worker;

    private LoadingCache<Pair<String, Integer>, HttpRequest> requestCache;

    public TansClient(String servers) {
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
                        //request.headers().set(HttpHeaderNames.HOST, addr.getLeft());
                        request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
                        request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.TEXT_PLAIN);

                        return request;
                    }
                });

        HttpClient client = selectServer("");
        for(int i = 0; i < 20; i++){
            client.createOneConnection();
        }

        while(client.connectionCount.get() < 10){
            try {
                Thread.sleep(500);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public LongRange acquire(String key, int n) throws TimeoutException {
        HttpRequest request;
        try {
            request = requestCache.get(Pair.of(key, n));
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        HttpClient client = selectServer(key);
        Channel channel = client.getChannel();

        CountDownLatch latch = new CountDownLatch(1);

        channel.attr(LATCH).set(latch);
        channel.writeAndFlush(request);

        try {
            latch.await(1, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (latch.getCount() > 0) {
            throw new TimeoutException();
        }

        LongRange r = channel.attr(RESULT).get();
        client.returnChannel(channel);

        return r;
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
}
