package ripple.server.tcp;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.nio.NioSocketChannel;
import ripple.common.tcp.MessageHandler;

import java.net.InetSocketAddress;

/**
 * @author Zhen Tang
 */
public class ServerMessageHandler extends MessageHandler {
    private NettyServer nettyServer;

    private NettyServer getNettyServer() {
        return nettyServer;
    }

    private void setNettyServer(NettyServer nettyServer) {
        this.nettyServer = nettyServer;
    }

    public ServerMessageHandler(NettyServer nettyServer) {
        this.setNettyServer(nettyServer);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        InetSocketAddress address = ((NioSocketChannel) ctx.channel()).remoteAddress();
        System.out.println("Connected: " + address.getHostString() + ":" + address.getPort());
        this.getNettyServer().getConnectedNodes().add(ctx.channel());
    }
}
