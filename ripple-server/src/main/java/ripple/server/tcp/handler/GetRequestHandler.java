// Copyright (c) 2024 Institute of Software, Chinese Academy of Sciences
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
import ripple.common.entity.AbstractMessage;
import ripple.common.entity.DeleteMessage;
import ripple.common.entity.IncrementalUpdateMessage;
import ripple.common.entity.Item;
import ripple.common.entity.UpdateMessage;
import ripple.common.tcp.Handler;
import ripple.common.tcp.Message;
import ripple.common.tcp.message.GetRequest;
import ripple.common.tcp.message.GetResponse;
import ripple.common.tcp.message.GetResponseItem;
import ripple.server.core.Node;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Zhen Tang
 */
public class GetRequestHandler implements Handler {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetRequestHandler.class);
    private Node node;

    private Node getNode() {
        return node;
    }

    private void setNode(Node node) {
        this.node = node;
    }

    public GetRequestHandler(Node node) {
        this.setNode(node);
    }

    @Override
    public Message handle(ChannelHandlerContext channelHandlerContext, Message message) {
        GetRequest getRequest = (GetRequest) message;
        InetSocketAddress localAddress = ((NioSocketChannel) channelHandlerContext.channel()).localAddress();
        InetSocketAddress remoteAddress = ((NioSocketChannel) channelHandlerContext.channel()).remoteAddress();

        LOGGER.info("[GetRequestHandler] [{}:{}<-->{}:{}] Receive GET request. UUID = {}, application name = {}, key = {}."
                , localAddress.getHostString(), localAddress.getPort(), remoteAddress.getHostString()
                , remoteAddress.getPort(), getRequest.getUuid(), getRequest.getApplicationName(), getRequest.getKey());

        Item item = this.getNode().get(getRequest.getApplicationName(), getRequest.getKey());
        List<AbstractMessage> messageList = this.getNode().getStorage().getMessageService()
                .findMessages(item.getApplicationName(), item.getKey());
        this.getNode().getResolver().merge(item, messageList);

        List<GetResponseItem> history = new ArrayList<>();
        for (AbstractMessage abstractMessage : messageList) {
            GetResponseItem elem = new GetResponseItem();
            if (abstractMessage instanceof UpdateMessage) {
                UpdateMessage updateMessage = (UpdateMessage) abstractMessage;
                elem.setMessageUuid(updateMessage.getUuid());
                elem.setOperationType(updateMessage.getType());
                elem.setApplicationName(updateMessage.getApplicationName());
                elem.setKey(updateMessage.getKey());
                elem.setValue(updateMessage.getValue());
                elem.setLastUpdate(updateMessage.getLastUpdate());
                elem.setLastUpdateServerId(updateMessage.getLastUpdateServerId());
            } else if (abstractMessage instanceof DeleteMessage) {
                DeleteMessage deleteMessage = (DeleteMessage) abstractMessage;
                elem.setMessageUuid(deleteMessage.getUuid());
                elem.setOperationType(deleteMessage.getType());
                elem.setApplicationName(deleteMessage.getApplicationName());
                elem.setKey(deleteMessage.getKey());
                elem.setLastUpdate(deleteMessage.getLastUpdate());
                elem.setLastUpdateServerId(deleteMessage.getLastUpdateServerId());
            } else if (abstractMessage instanceof IncrementalUpdateMessage) {
                IncrementalUpdateMessage incrementalUpdateMessage = (IncrementalUpdateMessage) abstractMessage;
                elem.setMessageUuid(incrementalUpdateMessage.getUuid());
                elem.setOperationType(incrementalUpdateMessage.getType());
                elem.setApplicationName(incrementalUpdateMessage.getApplicationName());
                elem.setKey(incrementalUpdateMessage.getKey());
                elem.setBaseMessageUuid(incrementalUpdateMessage.getBaseMessageUuid());
                elem.setAtomicOperation(incrementalUpdateMessage.getAtomicOperation());
                elem.setValue(incrementalUpdateMessage.getValue());
                elem.setLastUpdate(incrementalUpdateMessage.getLastUpdate());
                elem.setLastUpdateServerId(incrementalUpdateMessage.getLastUpdateServerId());
            }
            history.add(elem);
        }

        GetResponse getResponse = new GetResponse();
        getResponse.setUuid(getRequest.getUuid());
        getResponse.setApplicationName(getRequest.getApplicationName());
        getResponse.setKey(getRequest.getKey());
        getResponse.setValue(item.getValue());
        getResponse.setItems(history);

        LOGGER.info("[GetRequestHandler] [{}:{}<-->{}:{}] Send GET response. value = {}"
                , localAddress.getHostString(), localAddress.getPort()
                , remoteAddress.getHostString(), remoteAddress.getPort(), item.getValue());
        return getResponse;
    }
}