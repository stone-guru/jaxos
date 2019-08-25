package org.jaxos.netty;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.jaxos.JaxosConfig;
import org.jaxos.network.RequestSender;
import org.jaxos.network.SenderFactory;
import org.jaxos.network.protobuff.PaxosMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * @author gaoyuan
 * @sine 2019/8/24.
 */
public class NettySenderFactory implements SenderFactory {
    Logger logger = LoggerFactory.getLogger(NettySenderFactory.class);

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
                                    .addLast(new ClientHandler());
                        }
                    });

            JaxosConfig.Peer peer = config.getPeer(1);
            ChannelFuture future = bootstrap.connect(new InetSocketAddress(peer.address(), peer.port())).sync();

            return new NettySender(future);
        }
        catch (InterruptedException e) {
            logger.info("Interrupted");
            throw new RuntimeException("interrupted");
        }
    }

    private class ClientHandler extends ChannelInboundHandlerAdapter {
        Logger logger = LoggerFactory.getLogger(ClientHandler.class);

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
        private ChannelFuture channelFuture;

        public NettySender(ChannelFuture channelFuture) {
            this.channelFuture = channelFuture;
        }

        @Override
        public void broadcast() {
            ByteString body = PaxosMessage.PrepareReq.newBuilder()
                    .setBallot(520)
                    .build()
                    .toByteString();

            PaxosMessage.DataGram dataGram = PaxosMessage.DataGram.newBuilder()
                    .setInstanceId(100)
                    .setSender(2)
                    .setCode(PaxosMessage.Code.PREPARE_REQ)
                    .setBody(body)
                    .build();

            channelFuture.channel().writeAndFlush(dataGram);
        }
    }
}

