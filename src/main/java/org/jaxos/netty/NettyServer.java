package org.jaxos.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.*;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.jaxos.algo.Acceptor;
import org.jaxos.network.RequestDispatcher;
import org.jaxos.network.protobuff.PaxosMessage;
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
                    socketChannel.config().setAllocator(UnpooledByteBufAllocator.DEFAULT);

                    socketChannel.pipeline()
                            .addLast(new ProtobufVarint32FrameDecoder())
                            .addLast(new ProtobufDecoder(PaxosMessage.DataGram.getDefaultInstance()))
                            .addLast(new ProtobufVarint32LengthFieldPrepender())
                            .addLast(new ProtobufEncoder())
                            .addLast(new JaxosChannelHandler());
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
            PaxosMessage.DataGram response = requestDispatcher.process((PaxosMessage.DataGram)msg);
            if(response != null){
                ctx.writeAndFlush(response);
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
        NettyServer server = new NettyServer("192.168.43.35", 9999, 0);
        server.startup();
    }
}
