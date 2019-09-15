/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.axesoft.tans.server;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
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
import org.axesoft.jaxos.JaxosSettings;
import org.axesoft.jaxos.algo.Proponent;
import org.axesoft.jaxos.algo.ProposeResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private Proponent proponent;
    private TansConfig config;
    private Channel serverChannel;

    public HttpApiService(Proponent proponent, TansConfig config) {
        this.proponent = proponent;
        this.config = config;
        addListener(new ServiceListener(), MoreExecutors.directExecutor());
    }

    @Override
    protected void run() throws Exception {
        // Configure the server.
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.option(ChannelOption.SO_BACKLOG, 1024);
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new HttpHelloWorldServerInitializer());

            this.serverChannel = b.bind(config.httpPort()).sync().channel();

            this.serverChannel.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    @Override
    protected void triggerShutdown() {
        if(this.serverChannel != null){
            this.serverChannel.close();
        }
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
        private ProposeResult result;
        private final ByteBuf okText = Unpooled.copiedBuffer("OK\r\n", CharsetUtil.UTF_8);
        private AtomicInteger i = new AtomicInteger(0);

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        private ProposeResult propose(ChannelHandlerContext ctx, ByteString value) {
            try {
                int i = 0;
                ProposeResult r;
                do {
                    r = proponent.propose(value);
                    logger.debug("propose value {}", value.toStringUtf8());
                    i++;
                } while (!r.isSuccess() && i < 3);
                return r;
            } catch (InterruptedException e) {
                logger.info("Asked to be quit");
                return null;
            }
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof HttpRequest) {
                HttpRequest request = this.request = (HttpRequest) msg;

                if (HttpUtil.is100ContinueExpected(request)) {
                    send100Continue(ctx);
                }

                if (request.method().equals(HttpMethod.POST)) {
                    int v = i.incrementAndGet();
                    this.result = propose(ctx, ByteString.copyFromUtf8(" propose " + v));

                    if (this.result == null) {
                        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
                        return;
                    }
                }
            }

            if (msg instanceof LastHttpContent) {
                if (!writeResponse(this.result, ctx)) {
                    // If keep-alive is off, close the connection once the content is fully written.
                    ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
                }
            }
        }

        private boolean writeResponse(ProposeResult result, ChannelHandlerContext ctx) {
            FullHttpResponse response;
            if (result.isSuccess()) {
                long instanceId = (Long) result.param();
                ByteBuf buf = Unpooled.copiedBuffer("OK  " + instanceId + "\r\n", CharsetUtil.UTF_8);
                response = new DefaultFullHttpResponse(HTTP_1_1, OK, buf);
            }
            else if (result.code() == ProposeResult.Code.OTHER_LEADER) {
                int httpPort = config.getPeerHttpPort((int)result.param());

                JaxosSettings.Peer peer = (JaxosSettings.Peer) result.param();
                response = new DefaultFullHttpResponse(
                        HTTP_1_1, TEMPORARY_REDIRECT);
                response.headers().set(HttpHeaderNames.LOCATION, String.format("http://%s:%s", peer.address(), httpPort));
            }
            else {
                response = new DefaultFullHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR,
                        Unpooled.copiedBuffer(result.code().toString() + "\r\n", CharsetUtil.UTF_8));
            }

            boolean keepAlive = HttpUtil.isKeepAlive(request);

            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");

            if (keepAlive) {
                response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }

            // Write the response.
            ctx.write(response);

            return keepAlive;
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
}
