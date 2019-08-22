package org.jaxos.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.net.InetSocketAddress;

/**
 * @author gaoyuan
 * @sine 2019/8/22.
 */
public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    private String address;
    private int port;

    public Server(String address, int port) {
        this.address = address;
        this.port = port;
    }

    public void startup() {
        EventLoopGroup group = new NioEventLoopGroup();

        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(group);
            serverBootstrap.channel(NioServerSocketChannel.class);
            serverBootstrap.localAddress(new InetSocketAddress(this.address, this.port));

            serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                protected void initChannel(SocketChannel socketChannel) throws Exception {
                    socketChannel.pipeline().addLast(new HelloServerHandler());
                }
            });
            ChannelFuture channelFuture = serverBootstrap.bind().sync();
            channelFuture.channel().closeFuture().sync();
        }
        catch (Exception e) {
            logger.error("start server error", e);
        }
        finally {
            try {
                group.shutdownGracefully().sync();
            }
            catch (InterruptedException e) {
                logger.info("Interrupted");
            }
        }
    }

    public void shutdown() {

    }


    public static class HelloServerHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf inBuffer = (ByteBuf) msg;

            String received = inBuffer.toString(CharsetUtil.UTF_8);
            logger.info("Server received: " + received);

            ctx.write(Unpooled.copiedBuffer("Hello " + received, CharsetUtil.UTF_8));
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
                    .addListener(ChannelFutureListener.CLOSE);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("error when handle request", cause);
            ctx.close();
        }
    }

    public static void main(String[] args) {
        Server server = new Server("localhost", 9999);
        server.startup();
    }
}
