package org.jaxos.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.*;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.jaxos.algo.Acceptor;
import org.jaxos.network.RequestDispatcher;
import org.jaxos.network.protobuff.ProtoRequestDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * @author gaoyuan
 * @sine 2019/8/22.
 */
public class NettyServer {
    private static Logger logger = LoggerFactory.getLogger(NettyServer.class);

    private String address;
    private int port;
    private int serverId;

    private RequestDispatcher requestDispatcher;

    public NettyServer(String address, int port, int serverId) {
        this.address = address;
        this.port = port;
        this.serverId = serverId;
        this.requestDispatcher = new ProtoRequestDispatcher(serverId, new Acceptor());
    }

    public void startup() {
        EventLoopGroup group = new NioEventLoopGroup();

        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap()
                    .group(group)
                    .channel(NioServerSocketChannel.class)
                    .localAddress(new InetSocketAddress(this.address, this.port))
                    .option(ChannelOption.ALLOCATOR, UnpooledByteBufAllocator.DEFAULT)
                    .option(ChannelOption.RCVBUF_ALLOCATOR, AdaptiveRecvByteBufAllocator.DEFAULT);

            serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                protected void initChannel(SocketChannel socketChannel) throws Exception {
                    socketChannel.pipeline().addLast(new JaxosChannelHandler());
                    socketChannel.config().setAllocator(UnpooledByteBufAllocator.DEFAULT);
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


    public class JaxosChannelHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf inBuffer = (ByteBuf) msg;
            byte bx[] = new byte[inBuffer.readableBytes()];
            inBuffer.readBytes(bx);
            byte[] result = requestDispatcher.process(bx);
            if(result != null){
                ctx.writeAndFlush(Unpooled.copiedBuffer(result));
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.writeAndFlush(Unpooled.EMPTY_BUFFER);
                    //.addListener(ChannelFutureListener.CLOSE);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("error when handle request", cause);
            ctx.close();
        }
    }

    public static void main(String[] args) {
        NettyServer server = new NettyServer("localhost", 9999, 0);
        server.startup();
    }
}
