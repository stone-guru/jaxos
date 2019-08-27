package org.jaxos.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.jaxos.JaxosConfig;
import org.jaxos.algo.Event;
import org.jaxos.algo.EventEntryPoint;
import org.jaxos.algo.Communicator;
import org.jaxos.network.CommunicatorFactory;
import org.jaxos.network.protobuff.PaxosMessage;
import org.jaxos.network.protobuff.ProtoMessageCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;

/**
 * @author gaoyuan
 * @sine 2019/8/24.
 */
public class NettyCommunicatorFactory implements CommunicatorFactory {
    private static Logger logger = LoggerFactory.getLogger(NettyCommunicatorFactory.class);
    private JaxosConfig config;
    private ProtoMessageCoder coder;
    private EventEntryPoint localEventEntryPoint;
    private ChannelGroupCommunicator communicator;

    public NettyCommunicatorFactory(JaxosConfig config, EventEntryPoint eventEntryPoint) {
        this.config = config;
        this.coder = new ProtoMessageCoder(config);
        this.localEventEntryPoint = eventEntryPoint;
        this.communicator = new ChannelGroupCommunicator();
    }

    @Override
    public Communicator createSender() {
        EventLoopGroup worker = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap()
                    .group(worker)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .option(ChannelOption.RCVBUF_ALLOCATOR, AdaptiveRecvByteBufAllocator.DEFAULT)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            ChannelPipeline pipeline = socketChannel.pipeline();
                            pipeline.addLast(new LoggingHandler(LogLevel.DEBUG))
                                    .addLast(new ProtobufVarint32FrameDecoder())
                                    .addLast(new ProtobufDecoder(PaxosMessage.DataGram.getDefaultInstance()))
                                    .addLast(new ProtobufVarint32LengthFieldPrepender())
                                    .addLast(new ProtobufEncoder())
                                    .addLast(new JaxosOutboundHandler())
                                    .addLast(new JaxosClientHandler());
                        }
                    });

            for(JaxosConfig.Peer peer : config.peerMap().values()){
                bootstrap.connect(new InetSocketAddress(peer.address(), peer.port())).sync();
            }

            return this.communicator;
        }
        catch (InterruptedException e) {
            logger.info("Interrupted");
            throw new RuntimeException("interrupted");
        }
    }

    private class ChannelGroupCommunicator implements Communicator {
        private ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

        @Override
        public void broadcast(Event event) {
            Event ret = localEventEntryPoint.process(event);
            localEventEntryPoint.process(ret);

            PaxosMessage.DataGram dataGram = coder.encode(event);
            channels.writeAndFlush(dataGram);
        }

        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            Channel c = ctx.channel();
            logger.info("Connected to a server {}", c.remoteAddress());

            channels.add(ctx.channel());
        }

        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            Channel c = ctx.channel();
            logger.info("Disconnect from a server {}", c.remoteAddress());

            channels.remove(ctx.channel());
        }
    }

    private class JaxosClientHandler extends ChannelInboundHandlerAdapter{
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            communicator.channelActive(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            communicator.channelInactive(ctx);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if(msg instanceof PaxosMessage.DataGram) {
                PaxosMessage.DataGram dataGram = (PaxosMessage.DataGram) msg;
                //this is an empty dataGram
                if(dataGram.getCode() == PaxosMessage.Code.NONE){
                    return;
                }

                Event event = coder.decode(dataGram);
                if (event != null) {
                    localEventEntryPoint.process(event);
                }
            } else {
                logger.error("Unknown received object {}", Objects.toString(msg));
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }

    private class JaxosOutboundHandler extends ChannelOutboundHandlerAdapter {
        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
            logger.info("jaxos connect from {} to {}",localAddress, remoteAddress);
            super.connect(ctx, remoteAddress, localAddress, promise);
        }
    }
}

