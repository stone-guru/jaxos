package org.axesoft.tans.server;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
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
import org.axesoft.jaxos.base.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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

    //The service URL is http://<host>:<port>/acquire?key=<keyname>[&n=<number>]
    private static final String ACQUIRE_PATH = "/acquire";
    private static final String KEY_ARG_NAME = "key";
    private static final String NUM_ARG_NAME = "n";

    private static final String ARG_KEY_REQUIRED_MSG = "argument of '" + KEY_ARG_NAME + "' is required";

    private static final ByteBuf NOT_FOUND_BYTEBUF = Unpooled.copiedBuffer("Not Found", CharsetUtil.UTF_8);

    private TansService tansService;
    private TansConfig config;

    private Channel serverChannel;
    private PartedThreadPool threadPool;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    private RequestQueue[] requestQueues;
    private Timer queueTimer;

    private Derivator taskCounter;
    private Derivator requestCounter;
    private SlideCounter requestSlideCounter;

    public HttpApiService(TansConfig config, TansService tansService) {
        this.tansService = tansService;
        this.config = config;
        this.taskCounter = new Derivator();
        this.requestCounter = new Derivator();
        this.requestSlideCounter = new SlideCounter(1, SlideCounter.StatMethod.STAT_SUM, 1);

        super.addListener(new Listener() {
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

            @Override
            public void failed(State from, Throwable failure) {
                logger.error(String.format("%s failed %s due to exception", SERVICE_NAME, from), failure);
            }
        }, MoreExecutors.directExecutor());
    }

    @Override
    protected void run() throws Exception {
        this.threadPool = new PartedThreadPool(this.config.jaxConfig().partitionNumber());

        this.requestQueues = new RequestQueue[config.jaxConfig().partitionNumber()];
        for (int i = 0; i < requestQueues.length; i++) {
            this.requestQueues[i] = new RequestQueue(i, config.requestBatchSize());
        }
        this.queueTimer = new Timer("Queue Flush", true);
        this.queueTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                long current = System.currentTimeMillis();
                for (RequestQueue q : requestQueues) {
                    q.click(current);
                }
            }
        }, 1000, 1);

        this.bossGroup = new NioEventLoopGroup(1);
        this.workerGroup = new NioEventLoopGroup(3);
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.option(ChannelOption.SO_BACKLOG, 1024);
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.ERROR))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new HttpRequestDecoder());
                            p.addLast(new HttpResponseEncoder());
                            p.addLast(new HttpChannelHandler());
                        }
                    });

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
        long current = System.currentTimeMillis();
        Pair<Double, Double> requestDelta = this.requestCounter.compute(current);
        Pair<Double, Double> taskDelta = this.taskCounter.compute(this.threadPool.totalExecTimes(), current);
        long w = this.threadPool.lastMinuteWaitingSize();

        logger.info("More {} req, run {} times, req max {}/sec, avg {}/sec, proposal {}/sec, max waiting {}",
                requestDelta.getLeft().intValue(), taskDelta.getLeft().intValue(),
                requestSlideCounter.getMaximal(0), String.format("%.1f", requestDelta.getRight()),
                String.format("%.1f", taskDelta.getRight()), w);
    }


    private FullHttpResponse createResponse(HttpResponseStatus status, String v) {
        return new DefaultFullHttpResponse(HTTP_1_1, status, Unpooled.copiedBuffer(v + "\r\n", CharsetUtil.UTF_8));
    }

    private FullHttpResponse createRedirectResponse(int serverId, String key, long n) {
        int httpPort = config.getPeerHttpPort(serverId);
        JaxosSettings.Peer peer = config.jaxConfig().getPeer(serverId);
        String url = String.format("http://%s:%s%s?%s=%s&%s=%d",
                peer.address(), httpPort, ACQUIRE_PATH,
                KEY_ARG_NAME, key,
                NUM_ARG_NAME, n);

        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, TEMPORARY_REDIRECT);
        response.headers().set(HttpHeaderNames.LOCATION, url);
        return response;
    }


    private void writeResponse(ChannelHandlerContext ctx, boolean keepAlive, FullHttpResponse response) {
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

    public class HttpChannelHandler extends SimpleChannelInboundHandler<Object> {
        private HttpRequest request;
        private boolean keepAlive;

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof HttpRequest) {
                HttpRequest request = this.request = (HttpRequest) msg;
                this.keepAlive = HttpUtil.isKeepAlive(request);
                if (HttpUtil.is100ContinueExpected(request)) {
                    send100Continue(ctx);
                }
            }

            if (msg instanceof LastHttpContent) {
                if (ACQUIRE_PATH.equals(getRawUri(request.uri()))) {
                    Either<String, Pair<String, Long>> either = parseRequest(request);
                    if (!either.isRight()) {
                        writeResponse(ctx, keepAlive,
                                new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST,
                                        Unpooled.copiedBuffer(either.getLeft(), CharsetUtil.UTF_8)));
                        return;
                    }

                    final String key = either.getRight().getKey();
                    final long v = either.getRight().getValue();
                    int i = tansService.squadIdOf(key);

                    requestQueues[i].addRequest(key, v, this.keepAlive, ctx);
                }
                else {
                    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.NOT_FOUND,
                            NOT_FOUND_BYTEBUF);
                    writeResponse(ctx, keepAlive, response);
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

            List<String> vx = params.get(KEY_ARG_NAME);
            if (vx == null || vx.size() == 0) {
                return Either.left(ARG_KEY_REQUIRED_MSG);
            }
            String key = vx.get(0);
            if (Strings.isNullOrEmpty(key)) {
                return Either.left(ARG_KEY_REQUIRED_MSG);
            }

            long n = 1;
            List<String> nx = params.get(NUM_ARG_NAME);
            if (nx != null && nx.size() > 0) {
                try {
                    n = Long.parseLong(nx.get(0));
                    if (n <= 0) {
                        return Either.left("Zero or negative n '" + n + "'");
                    }
                }
                catch (NumberFormatException e) {
                    return Either.left("Invalid integer '" + nx.get(0) + "'");
                }
            }

            return Either.right(Pair.of(key, n));
        }

        private void send100Continue(ChannelHandlerContext ctx) {
            FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, CONTINUE, Unpooled.EMPTY_BUFFER);
            ctx.write(response);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            //TODO some exception like reset by peer can be ignored
            logger.warn("Server channel got exception", cause);
            ctx.close();
        }
    }

    private static class TansRequest {
        KeyLong keyLong;
        boolean keepAlive;
        ChannelHandlerContext ctx;

        public TansRequest(String key, long v, boolean keepAlive, ChannelHandlerContext ctx) {
            this.keyLong = new KeyLong(key, v);
            this.keepAlive = keepAlive;
            this.ctx = ctx;
        }

        KeyLong keyLong() {
            return this.keyLong;
        }
    }

    private class RequestQueue {
        private final int squadId;
        private final int waitingSize;

        private long t0;
        private List<TansRequest> tasks;

        public RequestQueue(int squadId, int waitingSize) {
            this.squadId = squadId;
            this.waitingSize = waitingSize;
            this.t0 = 0;
            this.tasks = new ArrayList<>(waitingSize);
        }

        private synchronized void addRequest(String key, long v, boolean keepAlive, ChannelHandlerContext ctx) {
            if (logger.isTraceEnabled()) {
                logger.trace("RequestQueue accept request {} {} {}", key, v, keepAlive);
            }
            HttpApiService.this.requestCounter.inc();
            HttpApiService.this.requestSlideCounter.recordAtNow(1);

            this.tasks.add(new TansRequest(key, v, keepAlive, ctx));
            if (this.tasks.size() == 1) {
                this.t0 = System.currentTimeMillis();
            }

            if (tasks.size() >= waitingSize) {
                processRequests();
            }
        }

        private synchronized void click(long current) {
            if (t0 > 0 && current - t0 >= 2) { //2 ms
                logger.trace("Queue processEvent waiting tasks when timeout");
                processRequests();
            }
        }

        private void processRequests() {
            final List<TansRequest> todo = ImmutableList.copyOf(this.tasks);
            this.tasks.clear();
            this.t0 = 0;

            threadPool.submit(this.squadId, () -> process(todo));
        }

        private void process(List<TansRequest> tasks) {
            List<KeyLong> requests = Lists.transform(tasks, TansRequest::keyLong);
            List<LongRange> rx = null;
            FullHttpResponse errorResponse = null;
            int otherLeaderId = -1;
            try {
                rx = tansService.acquire(squadId, requests);

                if (logger.isTraceEnabled()) {
                    for (LongRange r : rx) {
                        logger.trace("Acquire result [{},{}]", r.low(), r.high());
                    }
                }
            }
            catch (RedirectException e) {
                otherLeaderId = e.getServerId();
            }
            catch (ConflictException e) {
                errorResponse = createResponse(HttpResponseStatus.CONFLICT, "CONFLICT");
            }
            catch (Exception e) {
                logger.error("Process ", e);
                errorResponse = createResponse(INTERNAL_SERVER_ERROR, "INTERNAL ERROR");
            }

            for (int i = 0; i < requests.size(); i++) {
                TansRequest task = tasks.get(i);

                FullHttpResponse response;
                if (errorResponse != null) {
                    response = errorResponse;
                }
                else if (otherLeaderId >= 0) {
                    response = createRedirectResponse(otherLeaderId, task.keyLong.key(), task.keyLong.value());
                }
                else {
                    LongRange r = rx.get(i);
                    String content = task.keyLong.key() +", " + r.low() + "," + r.high();
                    response = createResponse(OK, content);
                }

                logger.trace("write response ");
                writeResponse(task.ctx, task.keepAlive, response);
            }
        }
    }

    private static class PartedThreadPool {
        private TansExecutor[] executors;

        private PartedThreadPool(int threadNum) {
            this.executors = new TansExecutor[threadNum];
            for (int i = 0; i < threadNum; i++) {
                this.executors[i] = new TansExecutor(i);
            }
        }

        private ListenableFuture<Void> submit(int i, Runnable r) {
            return this.executors[i].submit(() -> {
                r.run();
                return null;
            });
        }

        private <T> ListenableFuture<T> submit(int i, Callable<T> task) {
            return this.executors[i].submit(task);
        }

        private int totalExecTimes() {
            int t = 0;
            for (TansExecutor executor : this.executors) {
                t += executor.totalExecTimes();
            }
            return t;
        }

        private int lastMinuteWaitingSize() {
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

        private TansExecutor(int i) {
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

        private <T> ListenableFuture<T> submit(Callable<T> task) {
            ListenableFuture<T> f = this.executor.submit(task);
            this.execTimes.incrementAndGet();
            this.waitingTaskCounter.recordAtNow(this.threadPool.getQueue().size());
            return f;
        }

        private int totalExecTimes() {
            return this.execTimes.get();
        }

        private int lastMinuteWaitingSize() {
            return this.waitingTaskCounter.getMaximal(0);
        }
    }
}
