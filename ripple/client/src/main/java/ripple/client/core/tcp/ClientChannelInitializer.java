package ripple.client.core.tcp;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import ripple.common.tcp.MessageDecoder;
import ripple.common.tcp.MessageEncoder;
import ripple.common.tcp.MessageHandler;
import ripple.common.tcp.MessageType;
import ripple.common.tcp.decoder.HeartbeatRequestDecoder;
import ripple.common.tcp.decoder.HeartbeatResponseDecoder;
import ripple.common.tcp.encoder.HeartbeatRequestEncoder;
import ripple.common.tcp.encoder.HeartbeatResponseEncoder;

/**
 * @author Zhen Tang
 */
public class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {
    @Override
    protected void initChannel(SocketChannel socketChannel) {
        ChannelPipeline pipeline = socketChannel.pipeline();
        pipeline.addLast(new LengthFieldBasedFrameDecoder(1024, 0, 4, 0, 4));
        pipeline.addLast(new LengthFieldPrepender(4));
        MessageEncoder messageEncoder = new MessageEncoder();
        MessageDecoder messageDecoder = new MessageDecoder();
        MessageHandler messageHandler = new ClientMessageHandler();
        pipeline.addLast(messageEncoder);
        pipeline.addLast(messageDecoder);
        pipeline.addLast(messageHandler);

        messageEncoder.registerEncoder(MessageType.HEARTBEAT_REQUEST, new HeartbeatRequestEncoder());
        messageDecoder.registerDecoder(MessageType.HEARTBEAT_REQUEST, new HeartbeatRequestDecoder());
        messageHandler.registerHandler(MessageType.HEARTBEAT_REQUEST, new HeartbeatRequestHandler());

        messageEncoder.registerEncoder(MessageType.HEARTBEAT_RESPONSE, new HeartbeatResponseEncoder());
        messageDecoder.registerDecoder(MessageType.HEARTBEAT_RESPONSE, new HeartbeatResponseDecoder());
    }
}
