package org.jaxos.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.jaxos.JaxosConfig;
import org.jaxos.algo.*;
import org.jaxos.network.MessageCoder;
import org.jaxos.network.protobuff.PaxosMessage;
import org.jaxos.network.protobuff.ProtoMessageCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaoyuan
 * @sine 2019/8/22.
 */
public class NettyServer {
    private static Logger logger = LoggerFactory.getLogger(NettyServer.class);

    private EventEntryPoint eventEntryPoint;
    private MessageCoder<PaxosMessage.DataGram> messageCoder;
    private JaxosConfig config;
    private Acceptor acceptor;
    private Communicator communicator;
    private Proposal proposal;

    public NettyServer(JaxosConfig config) {
        this.config = config;
        this.acceptor = new Acceptor(this.config);
        this.proposal = new Proposal(this.config, () -> this.communicator);
        this.eventEntryPoint = new LocalEventCenter(this.proposal, this.acceptor);
        this.communicator = new NettyCommunicatorFactory(this.config, eventEntryPoint).createCommunicator();
        this.messageCoder = new ProtoMessageCoder(this.config);
    }

    public void startup() {
        EventLoopGroup group = new NioEventLoopGroup();

        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap()
                    .group(group)
                    .channel(NioServerSocketChannel.class)
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
            ChannelFuture channelFuture = serverBootstrap.bind(config.port()).sync();
            logger.warn("Jaxos server {} started at {}", config.serverId(), config.port());
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
            if(msg instanceof PaxosMessage.DataGram){
                Event e0 = messageCoder.decode((PaxosMessage.DataGram)msg);
                Event e1 = eventEntryPoint.process(e0);
                if(e1 != null){
                    PaxosMessage.DataGram response = messageCoder.encode(e1);
                    ctx.writeAndFlush(response);
                }
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

}
