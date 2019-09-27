package org.axesoft.jaxos.netty;

import com.google.common.util.concurrent.RateLimiter;
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
import org.axesoft.jaxos.algo.Event;
import org.axesoft.jaxos.algo.EventWorkerPool;
import org.axesoft.jaxos.network.CommunicatorFactory;
import org.axesoft.jaxos.network.protobuff.PaxosMessage;
import org.axesoft.jaxos.network.protobuff.ProtoMessageCoder;
import org.axesoft.jaxos.JaxosSettings;
import org.axesoft.jaxos.algo.Communicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author gaoyuan
 * @sine 2019/8/24.
 */
public class NettyCommunicatorFactory implements CommunicatorFactory {
    private static Logger logger = LoggerFactory.getLogger(NettyCommunicatorFactory.class);
    private JaxosSettings config;
    private ProtoMessageCoder coder;
    private ChannelGroupCommunicator communicator;
    private EventWorkerPool eventWorkerPool;

    public NettyCommunicatorFactory(JaxosSettings config, EventWorkerPool eventWorkerPool) {
        this.config = config;
        this.coder = new ProtoMessageCoder(config);
        this.eventWorkerPool = eventWorkerPool;
    }

    @Override
    public Communicator createCommunicator() {
        EventLoopGroup worker = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap()
                    .group(worker)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, true)
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
                                    //.addLast(new JaxosOutboundHandler())
                                    .addLast(new JaxosClientHandler());
                        }
                    });

            this.communicator = new ChannelGroupCommunicator(worker, bootstrap);
            this.communicator.start();
            return this.communicator;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private class ChannelGroupCommunicator implements Communicator {
        private static final long HEART_BEAT_INTERVAL_SEC = 2;
        private ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        private Bootstrap bootstrap;
        private EventLoopGroup worker;
        private RateLimiter logLimiter = RateLimiter.create(1.0 / 15);

        private Map<ChannelId, JaxosSettings.Peer> channelPeerMap = new ConcurrentHashMap<>();
        private Map<Integer, ChannelId> channelIdMap = new ConcurrentHashMap<>();

        private PaxosMessage.DataGram heartBeatDataGram = coder.encode(new Event.HeartBeatRequest(config.serverId()));

        public ChannelGroupCommunicator(EventLoopGroup worker, Bootstrap bootstrap) {
            this.worker = worker;
            this.bootstrap = bootstrap;

            this.worker.scheduleWithFixedDelay(() -> {
                        //logger.info("send heart beat");
                        channels.writeAndFlush(heartBeatDataGram);
                    },
                    HEART_BEAT_INTERVAL_SEC, HEART_BEAT_INTERVAL_SEC, TimeUnit.SECONDS);
        }

        public void start() {
            for (JaxosSettings.Peer peer : config.peerMap().values()) {
                if (peer.id() != config.serverId()) {
                    connect(peer);
                }
            }
        }

        private void connect(ChannelId channelId) {
            JaxosSettings.Peer peer = channelPeerMap.remove(channelId);
            if (peer == null) {
                logger.error("Peer for channel {} is not recorded", channelId);
            }
            else {
                connect(peer);
            }
        }

        private void connect(JaxosSettings.Peer peer) {
            ChannelFuture future = bootstrap.connect(new InetSocketAddress(peer.address(), peer.port()));
            future.addListener(f -> {
                if (!f.isSuccess()) {
                    if (logLimiter.tryAcquire()) {
                        logger.error("Unable to connect to {} ", peer);
                    }
                    worker.schedule(() -> connect(peer), 3, TimeUnit.SECONDS);
                }
                else {
                    logger.info("Connected to {}", peer);
                    Channel c = future.channel();

                    channels.add(c);
                    channelPeerMap.put(c.id(), peer);
                    channelIdMap.put(peer.id(), c.id());
                }
            });
        }

        @Override
        public boolean available() {
            return channels.size() + 1 >= config.peerCount() / 2;
        }

        @Override
        public void broadcast(Event event) {
            logger.trace("Broadcast {} " + event);
            PaxosMessage.DataGram dataGram = coder.encode(event);
            channels.writeAndFlush(dataGram);
            eventWorkerPool.submitToSelf(event);
        }

        @Override
        public void selfFirstBroadcast(Event event) {
            eventWorkerPool.directCallSelf(event);

            PaxosMessage.DataGram dataGram = coder.encode(event);
            ChannelId id = channelIdMap.get(config.serverId());
            channels.writeAndFlush(dataGram, c -> !c.id().equals(id));
        }

        @Override
        public void send(Event event, int serverId) {
            if (serverId == config.serverId()) {
                eventWorkerPool.submitToSelf(event);
            }
            else {
                PaxosMessage.DataGram dataGram = coder.encode(event);
                ChannelId id = channelIdMap.get(serverId);
                channels.writeAndFlush(dataGram, c -> c.id().equals(id));
            }
        }

        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            Channel c = ctx.channel();

            channels.remove(ctx.channel());
            JaxosSettings.Peer p = channelPeerMap.get(c.id());

            logger.error("Disconnected from a server {}", p);
            if (p != null) {
                channelIdMap.remove(p.id());
            }

            connect(c.id());
        }

        @Override
        public void close() {
            try {
                channels.close().sync();
                worker.shutdownGracefully().sync();
            }
            catch (InterruptedException e) {
                logger.info("Interrupted at communicator.close()");
            }
            catch (Exception e) {
                logger.error("error when do communicator.close()", e);
            }
        }
    }

    private class JaxosClientHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            communicator.channelInactive(ctx);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof PaxosMessage.DataGram) {
                PaxosMessage.DataGram dataGram = (PaxosMessage.DataGram) msg;
                //this is an empty dataGram
                //TODO ingest why
                if (dataGram.getCode() == PaxosMessage.Code.NONE) {
                    return;
                }

                Event event = coder.decode(dataGram);
                if (event != null) {
                    if (event.code() == Event.Code.HEART_BEAT_RESPONSE) {
                        //logger.info("Got heart beat response from server {}", event.senderId());
                    }
                    else {
                        eventWorkerPool.submitToSelf(event);
                    }
                }
            }
            else {
                logger.error("Unknown received object {}", Objects.toString(msg));
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("error ", cause);
            //cause.printStackTrace();
            ctx.close();
        }
    }
}

