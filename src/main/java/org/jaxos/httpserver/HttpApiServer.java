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
package org.jaxos.httpserver;

import com.google.protobuf.ByteString;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.CharsetUtil;
import org.jaxos.algo.Instance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpHeaderValues.TEXT_PLAIN;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * An HTTP server that sends back the content of the received HTTP request
 * in a pretty plaintext form.
 */
public final class HttpApiServer {
    private static final Logger logger = LoggerFactory.getLogger(HttpApiServer.class);

    private Instance instance;
    private int port;

    public HttpApiServer(Instance instance, int port) {
        this.instance = instance;
        this.port = port;
    }

    public void start() throws Exception {
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

            Channel ch = b.bind(port).sync().channel();

            System.err.println("Open your web browser and navigate to " +
                    "http://127.0.0.1:" + port + '/');

            ch.closeFuture().sync();
        }
        finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public class HttpHelloWorldServerInitializer extends ChannelInitializer<SocketChannel> {

        public HttpHelloWorldServerInitializer() {

        }

        @Override
        public void initChannel(SocketChannel ch) {
            ChannelPipeline p = ch.pipeline();
            p.addLast(new HttpRequestDecoder());
            p.addLast(new HttpResponseEncoder());
            p.addLast(new HttpSnoopServerHandler());
        }
    }

    public class HttpSnoopServerHandler extends SimpleChannelInboundHandler<Object> {

        private HttpRequest request;
        /**
         * Buffer that stores the response content
         */
        private final StringBuilder buf = new StringBuilder();

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof HttpRequest) {
                HttpRequest request = this.request = (HttpRequest) msg;

                if (HttpUtil.is100ContinueExpected(request)) {
                    send100Continue(ctx);
                }

                buf.setLength(0);
//                buf.append("METHOD: ").append(request.method()).append("\r\n");
//                buf.append("REQUEST_URI: ").append(request.uri()).append("\r\n");
//
//                QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.uri());
//                buf.append("RAW_PATH: ").append(queryStringDecoder.rawPath()).append("\r\n\r\n");
//                Map<String, List<String>> params = queryStringDecoder.parameters();

                if(request.method().equals(HttpMethod.POST)){
                    try {
                        instance.propose(ByteString.copyFromUtf8("Hello word!"));
                        buf.append("OK\r\n");
                    }catch(InterruptedException e){
                        logger.info("Asked to be quit");
                        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
                        return;
                    }
                }
//                if (!params.isEmpty()) {
//                    for (Map.Entry<String, List<String>> p : params.entrySet()) {
//                        String key = p.getKey();
//                        List<String> vals = p.getValue();
//                        for (String val : vals) {
//                            buf.append("PARAM: ").append(key).append(" = ").append(val).append("\r\n");
//                        }
//                    }
//                    buf.append("\r\n");
//                }
            }

            if (msg instanceof LastHttpContent) {
                //buf.append("END OF CONTENT\r\n");

                LastHttpContent trailer = (LastHttpContent) msg;
//                if (!trailer.trailingHeaders().isEmpty()) {
//                    buf.append("\r\n");
//                    for (CharSequence name : trailer.trailingHeaders().names()) {
//                        for (CharSequence value : trailer.trailingHeaders().getAll(name)) {
//                            buf.append("TRAILING HEADER: ");
//                            buf.append(name).append(" = ").append(value).append("\r\n");
//                        }
//                    }
//                    buf.append("\r\n");
//                }

                if (!writeResponse(trailer, ctx)) {
                    // If keep-alive is off, close the connection once the content is fully written.
                    ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
                }
            }
        }


        private boolean writeResponse(HttpObject currentObj, ChannelHandlerContext ctx) {
            // Decide whether to close the connection or not.
            if (currentObj == null) {
                //ByteBuf addr = Unpooled.copiedBuffer("Location: http://www.baidu.com", CharsetUtil.UTF_8);
                FullHttpResponse response = new DefaultFullHttpResponse(
                        HTTP_1_1, TEMPORARY_REDIRECT);
                response.headers().set(HttpHeaderNames.LOCATION, "http://www.baidu.com");
                ctx.writeAndFlush(response);
                return false;
            }

            boolean keepAlive = HttpUtil.isKeepAlive(request);
            // Build the response object.
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HTTP_1_1, currentObj.decoderResult().isSuccess() ? OK : BAD_REQUEST,
                    Unpooled.copiedBuffer(buf.toString(), CharsetUtil.UTF_8));

            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");

            if (keepAlive) {
                // Add 'Content-Length' header only for a keep-alive connection.
                response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
                // Add keep alive header as per:
                // - http://www.w3.org/Protocols/HTTP/1.1/draft-ietf-http-v11-spec-01.html#Connection
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
}
