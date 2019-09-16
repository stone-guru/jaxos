package org.axesoft.jaxos.netty;

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
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.axesoft.jaxos.JaxosSettings;
import org.axesoft.jaxos.algo.*;
import org.axesoft.jaxos.network.MessageCoder;
import org.axesoft.jaxos.network.protobuff.PaxosMessage;
import org.axesoft.jaxos.network.protobuff.ProtoMessageCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author gaoyuan
 * @sine 2019/8/22.
 */
public class NettyJaxosNode {
    private static Logger logger = LoggerFactory.getLogger(NettyJaxosNode.class);

    private JaxosSettings settings;
    private Map<ChannelId, JaxosSettings.Peer> channelPeerMap = new ConcurrentHashMap<>();
    private MessageCoder<PaxosMessage.DataGram> messageCoder;
    private Communicator communicator;
    private Supplier<EventDispatcher> eventDispatcherSupplier;
    private Channel serverChannel;

    public NettyJaxosNode(JaxosSettings settings, Supplier<EventDispatcher> eventDispatcherSupplier) {
        this.settings = settings;
        this.messageCoder = new ProtoMessageCoder(this.settings);
        this.eventDispatcherSupplier = eventDispatcherSupplier;
    }

    public void startup() {
        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup worker = new NioEventLoopGroup(4);
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap()
                    .group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.ALLOCATOR, UnpooledByteBufAllocator.DEFAULT)
                    .option(ChannelOption.RCVBUF_ALLOCATOR, AdaptiveRecvByteBufAllocator.DEFAULT);

            serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                protected void initChannel(SocketChannel socketChannel) throws Exception {
                    socketChannel.config().setAllocator(UnpooledByteBufAllocator.DEFAULT);

                    socketChannel.pipeline()
                            .addLast(new IdleStateHandler(20, 20, 20 * 10, TimeUnit.SECONDS))
                            .addLast(new ProtobufVarint32FrameDecoder())
                            .addLast(new ProtobufDecoder(PaxosMessage.DataGram.getDefaultInstance()))
                            .addLast(new ProtobufVarint32LengthFieldPrepender())
                            .addLast(new ProtobufEncoder())
                            .addLast(new JaxosChannelHandler());
                }
            });
            ChannelFuture channelFuture = serverBootstrap.bind(settings.self().port()).sync();
            this.serverChannel = channelFuture.channel();
            channelFuture.channel().closeFuture().sync();
        }
        catch (Exception e) {
            logger.error("start server error", e);
        }
        finally {
            try {
                worker.shutdownGracefully().sync();
                boss.shutdownGracefully().sync();
            }
            catch (InterruptedException e) {
                logger.info("Interrupted");
            }
        }
    }

    public void shutdown() {
        this.serverChannel.close();
    }


    public class JaxosChannelHandler extends ChannelInboundHandlerAdapter {
        private final PaxosMessage.DataGram heartBeatResponse = messageCoder.encode(new Event.HeartBeatResponse(settings.serverId()));

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            logger.info("Channel connected from {}", ctx.channel().remoteAddress());
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if(msg instanceof PaxosMessage.DataGram){
                Event e0 = messageCoder.decode((PaxosMessage.DataGram)msg);
                if(e0.code() == Event.Code.HEART_BEAT){
                    //logger.info("Receive heart beat from server {}", e0.senderId());
                    ctx.writeAndFlush(heartBeatResponse);
                } else {
                    EventDispatcher dispatcher = eventDispatcherSupplier.get();
                    if(dispatcher == null){
                        logger.warn("System not read abandon event", e0);
                        return;
                    }
                    Event e1 = dispatcher.process(e0);
                    if (e1 != null && e0.senderId() != -1) { //-1 is nothing, or other server's id
                        PaxosMessage.DataGram response = messageCoder.encode(e1);
                        ctx.writeAndFlush(response);
                    }
                }
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.writeAndFlush(Unpooled.EMPTY_BUFFER);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("error when handle request", cause);
            ctx.close();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object obj) throws Exception {
            super.userEventTriggered(ctx, obj);
            if(obj instanceof IdleStateEvent){
                IdleStateEvent event = (IdleStateEvent)obj;
                if (event.state() == IdleState.READER_IDLE || event.state() == IdleState.ALL_IDLE){
                    logger.warn("connection from {} idle, close it", channelPeerMap.get(ctx.channel().id()));
                    ctx.channel().close();
                }
            }
        }
    }

}
