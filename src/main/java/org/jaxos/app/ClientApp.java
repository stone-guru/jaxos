/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.jaxos.app;

import com.google.protobuf.ByteString;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.CharsetUtil;
import org.jaxos.JaxosConfig;
import org.jaxos.algo.Communicator;
import org.jaxos.algo.Instance;
import org.jaxos.netty.NettyCommunicatorFactory;

import java.net.URI;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientApp {
    static final String URL = "http://127.0.0.1:8081";

    public static void main(String[] args) throws Exception {
        ClientApp app = new ClientApp();
        app.run();
    }

    private HttpRequest request;
    private int n = 50000;
    private long start = 0;
    private AtomicInteger count = new AtomicInteger(0);

    private HttpRequest createRequest(URI uri, String host) {
        HttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.POST, uri.getRawPath(), Unpooled.EMPTY_BUFFER);
        request.headers().set(HttpHeaderNames.HOST, host);
        request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.TEXT_PLAIN);

        return request;
    }

    public void run() throws Exception {
        URI uri = new URI(URL);
        String host = uri.getHost() == null? "127.0.0.1" : uri.getHost();
        int port = uri.getPort();

        this.request = createRequest(uri, host);

        // Configure the client.
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new HttpSnoopClientInitializer());

            // Make the connection attempt.
            Channel ch = b.connect(host, port).sync().channel();

            // Prepare the HTTP request.

            this.start = System.nanoTime();
                // Send the HTTP request.
            ch.writeAndFlush(request);

            // Wait for the server to close the connection.
            ch.closeFuture().sync();
        } finally {
            // Shut down executor threads to exit.
            group.shutdownGracefully();
        }
    }

    public class HttpSnoopClientInitializer extends ChannelInitializer<SocketChannel> {

        @Override
        public void initChannel(SocketChannel ch) {
            ChannelPipeline p = ch.pipeline();
            p.addLast(new HttpClientCodec());
            // Remove the following line if you don't want automatic content decompression.
            p.addLast(new HttpContentDecompressor());
            // Uncomment the following line if you don't want to handle HttpContents.
            //p.addLast(new HttpObjectAggregator(1048576));
            p.addLast(new HttpSnoopClientHandler());
        }
    }

    public class HttpSnoopClientHandler extends SimpleChannelInboundHandler<HttpObject> {

        @Override
        public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
            if (msg instanceof HttpResponse) {
                HttpResponse response = (HttpResponse) msg;

                //System.err.println("STATUS: " + response.status());
            }
            if (msg instanceof HttpContent) {
                HttpContent content = (HttpContent) msg;

                System.err.print(content.content().toString(CharsetUtil.UTF_8));

                int i = count.incrementAndGet();
                if(i < n){
                    ctx.writeAndFlush(request);
                } else {
                    ctx.close();
                    double millis = (System.nanoTime() - start) / 1e+6;
                    double qps = n / (millis/1000.0);
                    System.out.println(String.format("POST %d in %.3f millis, QPS is %.0f", n, millis, qps));
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }


}
