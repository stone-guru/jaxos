package org.axesoft.tans.server;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.CharsetUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.axesoft.jaxos.JaxosSettings;
import org.axesoft.jaxos.base.Derivator;
import org.axesoft.jaxos.base.Either;
import org.axesoft.jaxos.base.SlideCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * An HTTP server that sends back the content of the received HTTP request
 * in a pretty plaintext form.
 */
public final class HttpApiService extends AbstractExecutionThreadService {
    private static final Logger logger = LoggerFactory.getLogger(HttpApiService.class);

    private static final String SERVICE_NAME = "Take-A-Number System";
    private TansService tansService;
    private TansConfig config;


    private Channel serverChannel;
    private PartedThreadPool threadPool;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Derivator taskCountDerivator;


    public HttpApiService(TansConfig config, TansService tansService) {
        this.tansService = tansService;
        this.config = config;

        addListener(new ServiceListener(), MoreExecutors.directExecutor());
    }

    @Override
    protected void run() throws Exception {
        this.taskCountDerivator = new Derivator();
        this.threadPool = new PartedThreadPool(this.config.jaxConfig().partitionNumber());


        this.bossGroup = new NioEventLoopGroup(1);
        this.workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.option(ChannelOption.SO_BACKLOG, 1024);
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.ERROR))
                    .childHandler(new HttpHelloWorldServerInitializer());

            this.serverChannel = b.bind(config.httpPort()).sync().channel();

            this.serverChannel.closeFuture().sync();
        }
        finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    @Override
    protected void triggerShutdown() {
        if (this.serverChannel != null) {
            this.serverChannel.close();
        }
    }

    public void printMetrics() {
        long t = this.threadPool.totalExecTimes();
        Pair<Double, Double> p = this.taskCountDerivator.accept(t, System.currentTimeMillis());
        long w = this.threadPool.lastMinuteWaitingSize();

        logger.info("{} get more {} tasks , speed {}/sec, max waiting tasks of last minute is {}",
                SERVICE_NAME, p.getLeft().intValue(), String.format("%.1f", p.getRight()), w);
    }

    public class HttpHelloWorldServerInitializer extends ChannelInitializer<SocketChannel> {
        @Override
        public void initChannel(SocketChannel ch) {
            ChannelPipeline p = ch.pipeline();
            p.addLast(new HttpRequestDecoder());
            p.addLast(new HttpResponseEncoder());
            p.addLast(new HttpChannelHandler());
        }
    }

    public class HttpChannelHandler extends SimpleChannelInboundHandler<Object> {
        private HttpRequest request;

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }


        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof HttpRequest) {
                HttpRequest request = this.request = (HttpRequest) msg;
                logger.trace("Got http request {}", request);

                if (HttpUtil.is100ContinueExpected(request)) {
                    send100Continue(ctx);
                }
            }

            if (msg instanceof LastHttpContent) {
                if (getRawUri(request.uri()).equals("/acquire")) {
                    Either<String, Pair<String, Long>> either = parseRequest(request);
                    if (!either.isRight()) {
                        writeResponse(ctx,
                                new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST,
                                        Unpooled.copiedBuffer(either.getLeft(), CharsetUtil.UTF_8)));
                        return;
                    }

                    final String key = either.getRight().getKey();
                    final long v = either.getRight().getValue();
                    int i = tansService.squadIdOf(key);

                    ListenableFuture<FullHttpResponse> future = threadPool.submit(i, () -> handleAcquire(key, v));
                    future.addListener(() -> {
                                FullHttpResponse response;
                                try {
                                    response = future.get();
                                    writeResponse(ctx, response);
                                }
                                catch (ExecutionException e) {
                                    logger.error("Exception", e);
                                    writeResponse(ctx, createResponse(INTERNAL_SERVER_ERROR, e.getMessage()));
                                }
                                catch (InterruptedException e) {
                                    //No response needed
                                    logger.info("Execute interrupted");
                                }
                            },
                            MoreExecutors.directExecutor());
                }
                else {
                    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.NOT_FOUND,
                            Unpooled.copiedBuffer("Not Found", CharsetUtil.UTF_8));
                    writeResponse(ctx, response);
                }
            }
        }

        private String getRawUri(String s) {
            int i = s.indexOf("?");
            if (i < 0) {
                return s;
            }
            else {
                return s.substring(0, i);
            }
        }

        private Either<String, Pair<String, Long>> parseRequest(HttpRequest request) {
            QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.uri());
            Map<String, List<String>> params = queryStringDecoder.parameters();

            List<String> vx = params.get("key");
            if (vx == null || vx.size() == 0) {
                return Either.left("Argument 'key' required");
            }
            String key = vx.get(0);
            if (Strings.isNullOrEmpty(key)) {
                return Either.left("Value of 'key' required");
            }

            long n = 1;
            List<String> nx = params.get("n");
            if (nx != null && nx.size() > 0) {
                try {
                    n = Long.parseLong(nx.get(0));
                    if (n <= 0) {
                        return Either.left("Zero or negative n " + n);
                    }
                }
                catch (NumberFormatException e) {
                    return Either.left(e.getMessage());
                }
            }

            return Either.right(Pair.of(key, n));
        }

        private FullHttpResponse handleAcquire(String key, long n) {
            try {
                Pair<Long, Long> p = tansService.acquire(key, n);
                String content = p.getLeft() + "," + p.getRight();

                return createResponse(OK, content);
            }
            catch (IllegalArgumentException e) {
                return createResponse(BAD_REQUEST, e.getMessage());
            }
            catch (RedirectException e) {
                return createRedirectResponse(e.getServerId(), key, n);
            }
            catch (ConflictException e) {
                return createResponse(HttpResponseStatus.CONFLICT, "CONFLICT");
            }
            catch (Exception e) {
                logger.error("Process " + key + ", " + n, e);
                return createResponse(INTERNAL_SERVER_ERROR, "INTERNAL ERROR");
            }
        }

        private FullHttpResponse createResponse(HttpResponseStatus status, String v) {
            return new DefaultFullHttpResponse(HTTP_1_1, status, Unpooled.copiedBuffer(v + "\r\n", CharsetUtil.UTF_8));
        }

        private FullHttpResponse createRedirectResponse(int serverId, String key, long n) {
            int httpPort = config.getPeerHttpPort(serverId);
            JaxosSettings.Peer peer = config.jaxConfig().getPeer(serverId);
            String url = String.format("http://%s:%s/acquire?key=%s&n=%d", peer.address(), httpPort, key, n);

            FullHttpResponse response = new DefaultFullHttpResponse(
                    HTTP_1_1, TEMPORARY_REDIRECT);
            response.headers().set(HttpHeaderNames.LOCATION, url);
            return response;
        }

        private void writeResponse(ChannelHandlerContext ctx, FullHttpResponse response) {
            boolean keepAlive = HttpUtil.isKeepAlive(request);

            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
            response.headers().set(HttpHeaderNames.HOST, config.address());
            response.headers().set(HttpHeaderNames.FROM, Integer.toString(config.httpPort()));
            response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());

            if (keepAlive) {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }

            ChannelFuture future = ctx.writeAndFlush(response);
            if (!keepAlive) {
                future.addListener(ChannelFutureListener.CLOSE);
            }
        }

        private void send100Continue(ChannelHandlerContext ctx) {
            FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, CONTINUE, Unpooled.EMPTY_BUFFER);
            ctx.write(response);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }

    private class ServiceListener extends Listener {
        @Override
        public void running() {
            logger.info("{} started at http://{}:{}/", SERVICE_NAME, config.address(), config.httpPort());
        }

        @Override
        public void stopping(State from) {
            logger.info("{} stopping HTTP API server", SERVICE_NAME);
        }

        @Override
        public void terminated(State from) {
            logger.info("{} terminated from state {}", SERVICE_NAME, from);
        }
    }

    private static class PartedThreadPool {
        private TansExecutor[] executors;

        public PartedThreadPool(int threadNum) {
            this.executors = new TansExecutor[threadNum];
            for (int i = 0; i < threadNum; i++) {
                this.executors[i] = new TansExecutor(i);
            }
        }

        public <T> ListenableFuture<T> submit(int i, Callable<T> task) {
            return this.executors[i].submit(task);
        }

        public int totalExecTimes() {
            int t = 0;
            for (TansExecutor executor : this.executors) {
                t += executor.totalExecTimes();
            }
            return t;
        }

        public int lastMinuteWaitingSize() {
            int t = 0;
            for (TansExecutor executor : this.executors) {
                t = Math.max(executor.lastMinuteWaitingSize(), t);
            }
            return t;
        }
    }

    private static class TansExecutor {
        private ThreadPoolExecutor threadPool;
        private ListeningExecutorService executor;
        private SlideCounter waitingTaskCounter;
        private AtomicInteger execTimes;

        public TansExecutor(int i) {
            this.threadPool = new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(),
                    (r) -> {
                        String name = "API Service Thread-" + i;
                        Thread thread = new Thread(r, name);
                        thread.setDaemon(true);
                        return thread;
                    });
            this.executor = MoreExecutors.listeningDecorator(this.threadPool);
            this.waitingTaskCounter = new SlideCounter(1, SlideCounter.StatMethod.STAT_MAX, 1);
            this.execTimes = new AtomicInteger(0);
        }

        public <T> ListenableFuture<T> submit(Callable<T> task) {
            ListenableFuture<T> f = this.executor.submit(task);
            this.execTimes.incrementAndGet();
            this.waitingTaskCounter.recordAtNow(this.threadPool.getQueue().size());
            return f;
        }

        public int totalExecTimes() {
            return this.execTimes.get();
        }

        public int lastMinuteWaitingSize() {
            return this.waitingTaskCounter.getMaximal(0);
        }
    }
}
