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
import ripple.common.tcp.message.IncrementalUpdateRequest;
import ripple.common.tcp.message.IncrementalUpdateResponse;
import ripple.server.core.Node;

import java.net.InetSocketAddress;

/**
 * @author Zhen Tang
 */
public class IncrementalUpdateRequestHandler implements Handler {
    private static final Logger LOGGER = LoggerFactory.getLogger(PutRequestHandler.class);
    private Node node;

    private Node getNode() {
        return node;
    }

    private void setNode(Node node) {
        this.node = node;
    }

    public IncrementalUpdateRequestHandler(Node node) {
        this.setNode(node);
    }

    @Override
    public Message handle(ChannelHandlerContext channelHandlerContext, Message message) {
        IncrementalUpdateRequest incrementalUpdateRequest = (IncrementalUpdateRequest) message;
        InetSocketAddress localAddress = ((NioSocketChannel) channelHandlerContext.channel()).localAddress();
        InetSocketAddress remoteAddress = ((NioSocketChannel) channelHandlerContext.channel()).remoteAddress();

        LOGGER.info("[IncrementalUpdateRequestHandler] [{}:{}<-->{}:{}] Receive INCREMENTAL_UPDATE request. UUID = {}" +
                        ", application name = {}, key = {}, base message uuid = {}, atomic operation = {}, value = {}."
                , localAddress.getHostString(), localAddress.getPort(), remoteAddress.getHostString()
                , remoteAddress.getPort(), incrementalUpdateRequest.getUuid(), incrementalUpdateRequest.getApplicationName()
                , incrementalUpdateRequest.getKey(), incrementalUpdateRequest.getBaseMessageUuid()
                , incrementalUpdateRequest.getAtomicOperation(), incrementalUpdateRequest.getValue());


        boolean result = this.getNode().incrementalUpdate(incrementalUpdateRequest.getApplicationName()
                , incrementalUpdateRequest.getKey(), incrementalUpdateRequest.getBaseMessageUuid()
                , incrementalUpdateRequest.getAtomicOperation(), incrementalUpdateRequest.getValue());

        IncrementalUpdateResponse incrementalUpdateResponse = new IncrementalUpdateResponse();
        incrementalUpdateResponse.setUuid(incrementalUpdateRequest.getUuid());
        incrementalUpdateResponse.setSuccess(result);

        LOGGER.info("[IncrementalUpdateRequestHandler] [{}:{}<-->{}:{}] Send INCREMENTAL_UPDATE response. Success = {}."
                , localAddress.getHostString(), localAddress.getPort(), remoteAddress.getHostString()
                , remoteAddress.getPort(), incrementalUpdateResponse.isSuccess());
        return incrementalUpdateResponse;
    }
}