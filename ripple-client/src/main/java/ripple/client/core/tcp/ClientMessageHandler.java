// Copyright (c) 2023 Institute of Software, Chinese Academy of Sciences
// Ripple is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ripple.client.core.tcp;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ripple.client.RippleClient;
import ripple.common.entity.ClientMetadata;
import ripple.common.entity.NodeMetadata;
import ripple.common.tcp.MessageHandler;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Zhen Tang
 */
public class ClientMessageHandler extends MessageHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientMessageHandler.class);
    private RippleClient rippleClient;

    private RippleClient getRippleClient() {
        return rippleClient;
    }

    private void setRippleClient(RippleClient rippleClient) {
        this.rippleClient = rippleClient;
    }

    public ClientMessageHandler(RippleClient rippleClient) {
        this.setRippleClient(rippleClient);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        InetSocketAddress localAddress = ((NioSocketChannel) ctx.channel()).localAddress();
        InetSocketAddress remoteAddress = ((NioSocketChannel) ctx.channel()).remoteAddress();
        LOGGER.info("[ClientMessageHandler] [{}:{}<-->{}:{}] Connected."
                , localAddress.getHostString(), localAddress.getPort()
                , remoteAddress.getHostString(), remoteAddress.getPort());
        boolean isServer = false;
        for (NodeMetadata nodeMetadata : this.getRippleClient().getMappingCache().values()) {
            if (nodeMetadata.getAddress().equals(remoteAddress.getHostString())
                    && nodeMetadata.getPort() == remoteAddress.getPort()) {
                isServer = true;
                this.getRippleClient().getServerConnections().put(nodeMetadata, ctx.channel());
                break;
            }
        }
        boolean found = false;
        if (!isServer) {
            for (ClientMetadata clientMetadata : this.getRippleClient().getClientConnections().keySet()) {
                if (clientMetadata.getAddress().equals(remoteAddress.getHostString())
                        && clientMetadata.getPort() == remoteAddress.getPort()) {
                    this.getRippleClient().getClientConnections().put(clientMetadata, ctx.channel());
                    found = true;
                    break;
                }
            }
        }
        if (!found) {
            this.getRippleClient().getClientConnections().put(
                    new ClientMetadata(remoteAddress.getHostString(), remoteAddress.getPort()), ctx.channel());
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        InetSocketAddress localAddress = ((NioSocketChannel) ctx.channel()).localAddress();
        InetSocketAddress remoteAddress = ((NioSocketChannel) ctx.channel()).remoteAddress();
        LOGGER.info("[ClientMessageHandler] [{}:{}<-->{}:{}] Disconnected."
                , localAddress.getHostString(), localAddress.getPort()
                , remoteAddress.getHostString(), remoteAddress.getPort());
        List<NodeMetadata> serverToRemove = new ArrayList<>();
        for (NodeMetadata nodeMetadata : this.getRippleClient().getServerConnections().keySet()) {
            if (this.getRippleClient().getServerConnections().get(nodeMetadata) == ctx.channel()) {
                serverToRemove.add(nodeMetadata);
            }
        }
        for (NodeMetadata nodeMetadata : serverToRemove) {
            this.getRippleClient().getServerConnections().remove(nodeMetadata);
        }
        List<ClientMetadata> clientToRemove = new ArrayList<>();
        for (ClientMetadata clientMetadata : this.getRippleClient().getClientConnections().keySet()) {
            if (this.getRippleClient().getClientConnections().get(clientMetadata) == ctx.channel()) {
                clientToRemove.add(clientMetadata);
            }
        }
        for (ClientMetadata clientMetadata : clientToRemove) {
            this.getRippleClient().getClientConnections().remove(clientMetadata);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        InetSocketAddress localAddress = ((NioSocketChannel) ctx.channel()).localAddress();
        InetSocketAddress remoteAddress = ((NioSocketChannel) ctx.channel()).remoteAddress();
        LOGGER.info("[ClientMessageHandler] [{}:{}<-->{}:{}] Exception caught: {}."
                , localAddress.getHostString(), localAddress.getPort()
                , remoteAddress.getHostString(), remoteAddress.getPort()
                , cause.getLocalizedMessage());
        cause.printStackTrace();
    }
}