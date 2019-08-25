package org.jaxos.netty;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.jaxos.JaxosConfig;
import org.jaxos.algo.Event;
import org.jaxos.network.RequestSender;
import org.jaxos.network.SenderFactory;
import org.jaxos.network.protobuff.PaxosMessage;
import org.jaxos.network.protobuff.ProtoMessageCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * @author gaoyuan
 * @sine 2019/8/24.
 */
public class NettySenderFactory implements SenderFactory {
    private static Logger logger = LoggerFactory.getLogger(NettySenderFactory.class);

    @Override
    public RequestSender createSender(JaxosConfig config) {
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
                            pipeline.addLast(new ProtobufVarint32FrameDecoder())
                                    .addLast(new ProtobufDecoder(PaxosMessage.DataGram.getDefaultInstance()))
                                    .addLast(new ProtobufVarint32LengthFieldPrepender())
                                    .addLast(new ProtobufEncoder())
                                    .addLast(new JaxosClientHandler());
                        }
                    });

            JaxosConfig.Peer peer = config.getPeer(0);
            ChannelFuture future = bootstrap.connect(new InetSocketAddress(peer.address(), peer.port())).sync();
            return new NettySender(config, future.channel());
        }
        catch (InterruptedException e) {
            logger.info("Interrupted");
            throw new RuntimeException("interrupted");
        }
    }

    private class JaxosClientHandler extends ChannelInboundHandlerAdapter {
        Logger logger = LoggerFactory.getLogger(JaxosClientHandler.class);

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            try {
                PaxosMessage.DataGram dataGram = (PaxosMessage.DataGram)msg;
                switch(dataGram.getCode()) {
                    case PREPARE_RES: {
                        PaxosMessage.PrepareRes res = PaxosMessage.PrepareRes.parseFrom(dataGram.getBody());
                        logger.info("prepare response, max ballot = {}, accepted ballot = {}, accepted value =",
                                res.getMaxBallot(), res.getAcceptedBallot(), res.getAcceptedValue());
                    }
                }
            }
            catch (InvalidProtocolBufferException e) {
                logger.error("error when get msg", e);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }

    private class NettySender implements RequestSender {
        private Channel channel;
        private JaxosConfig config;
        private ProtoMessageCoder coder;
        public NettySender(JaxosConfig config, Channel channel) {
            this.config = config;
            this.channel = channel;
            this.coder = new ProtoMessageCoder(this.config);
        }

        @Override
        public void broadcast(Event msg) {
            PaxosMessage.DataGram dataGram = this.coder.encode(msg);
            channel.writeAndFlush(dataGram);
        }
    }
}

