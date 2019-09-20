package org.axesoft.tans.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

/**
 * @author gaoyuan
 * @sine 2019/9/20.
 */
public class HttpClientPool {
    private static final Logger logger = LoggerFactory.getLogger(HttpClientPool.class);

    private final NioEventLoopGroup group;
    private final ConcurrentMap<InetSocketAddress, HttpClient> clientMap;
    private final BiFunction<HttpResponse, HttpContent, Optional<HttpRequest>> responseHandler;

    public HttpClientPool(List<InetSocketAddress> addresses, int connectionNumber, BiFunction<HttpResponse, HttpContent, Optional<HttpRequest>> handler) {
        group = new NioEventLoopGroup();
        responseHandler = handler;
        clientMap = new ConcurrentHashMap<>();
        for (InetSocketAddress address : addresses) {
            HttpClient client = getOrCreateHttpClient(address);
            for (int i = 0; i < connectionNumber; i++) {
                client.addOneConnection();
            }
        }
    }

    public void run(Map<InetSocketAddress, HttpRequest> initRequests) {
        for(Map.Entry<InetSocketAddress, HttpRequest> entry : initRequests.entrySet()){
            HttpClient client = getOrCreateHttpClient(entry.getKey());
            client.writeAndFlush(entry.getValue());
        }
    }

    public HttpClient getOrCreateHttpClient(InetSocketAddress address) {
        HttpClient client = clientMap.get(address);
        if (client == null) {
            synchronized (this.clientMap) {
                client = clientMap.get(address);
                if (client == null) {
                    client = new HttpClient(this.group, address.getHostName(), address.getPort());
                    this.clientMap.put(address, client);
                }
            }
        }
        return client;
    }

    private class HttpClient {
        private final String host;
        private final int port;

        private Bootstrap bootstrap;
        private ChannelGroup channels;

        HttpClient(NioEventLoopGroup worker, String host, int port) {
            this.host = host;
            this.port = port;
            this.channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
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
                            p.addLast(new HttpClientHandler(HttpClientPool.this.responseHandler));
                        }
                    });
        }

        ChannelFuture addOneConnection() {
            ChannelFuture future = this.bootstrap.connect(host, port);
            future.addListener(f -> {
                if (f.isSuccess()) {
                    logger.info("connected to {}:{}", host, port);
                    Channel channel = ((ChannelFuture) f).channel();
                    channels.add(channel);
                }
                else {
                    logger.error("failed to connect to {}:{}", host, port);
                }
            });
            return future;
        }

        void writeAndFlush(Object obj){
            channels.writeAndFlush(obj);
        }
    }


    private static class HttpClientHandler extends SimpleChannelInboundHandler<HttpObject> {
        HttpResponse response;
        BiFunction<HttpResponse, HttpContent, Optional<HttpRequest>> handler;

        public HttpClientHandler(BiFunction<HttpResponse, HttpContent, Optional<HttpRequest>> handler) {
            this.handler = handler;
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
            if (msg instanceof HttpResponse) {
                response = (HttpResponse) msg;
                //System.err.println("STATUS: " + response.status());
            }
            if (msg instanceof HttpContent) {
                Optional<HttpRequest> opt = handler.apply(response, (HttpContent)msg);
                opt.ifPresent(req -> ctx.writeAndFlush(req));
            }
        }
    }
}

