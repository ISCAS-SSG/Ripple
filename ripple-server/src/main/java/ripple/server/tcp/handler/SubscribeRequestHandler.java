// Copyright (c) 2023 Institute of Software, Chinese Academy of Sciences
// Ripple is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ripple.server.tcp.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ripple.common.tcp.Handler;
import ripple.common.tcp.Message;
import ripple.common.tcp.message.SubscribeRequest;
import ripple.common.tcp.message.SubscribeResponse;
import ripple.server.core.Node;

import java.net.InetSocketAddress;

/**
 * @author Zhen Tang
 */
public class SubscribeRequestHandler implements Handler {
    private static final Logger LOGGER = LoggerFactory.getLogger(SubscribeRequestHandler.class);
    private Node node;

    private Node getNode() {
        return node;
    }

    private void setNode(Node node) {
        this.node = node;
    }

    public SubscribeRequestHandler(Node node) {
        this.setNode(node);
    }

    @Override
    public Message handle(ChannelHandlerContext channelHandlerContext, Message message) {
        SubscribeRequest subscribeRequest = (SubscribeRequest) message;
        InetSocketAddress localAddress = ((NioSocketChannel) channelHandlerContext.channel()).localAddress();
        InetSocketAddress remoteAddress = ((NioSocketChannel) channelHandlerContext.channel()).remoteAddress();

        LOGGER.info("[SubscribeRequestHandler] [{}:{}<-->{}:{}] Receive SUBSCRIBE request. UUID = {}" +
                        ", application name = {}, key = {}, callback address = {}, callback port = {}."
                , localAddress.getHostString(), localAddress.getPort(), remoteAddress.getHostString()
                , remoteAddress.getPort(), subscribeRequest.getUuid(), subscribeRequest.getApplicationName()
                , subscribeRequest.getKey(), subscribeRequest.getCallbackAddress(), subscribeRequest.getCallbackPort());

        this.getNode().subscribe(subscribeRequest.getCallbackAddress(), subscribeRequest.getCallbackPort()
                , subscribeRequest.getApplicationName(), subscribeRequest.getKey());

        SubscribeResponse subscribeResponse = new SubscribeResponse();
        subscribeResponse.setUuid(subscribeRequest.getUuid());
        subscribeResponse.setSuccess(true);

        LOGGER.info("[DeleteRequestHandler] [{}:{}<-->{}:{}] Send SUBSCRIBE response. Success = {}."
                , localAddress.getHostString(), localAddress.getPort(), remoteAddress.getHostString()
                , remoteAddress.getPort(), subscribeResponse.isSuccess());
        return subscribeResponse;
    }
}