package ripple.server.tcp.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ripple.common.entity.Constants;
import ripple.common.entity.DeleteMessage;
import ripple.common.entity.UpdateMessage;
import ripple.common.tcp.Handler;
import ripple.common.tcp.Message;
import ripple.common.tcp.message.SyncRequest;
import ripple.common.tcp.message.SyncResponse;
import ripple.server.core.Node;

import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;

/**
 * @author Zhen Tang
 */
public class SyncRequestHandler implements Handler {
    private static final Logger LOGGER = LoggerFactory.getLogger(SyncRequestHandler.class);

    private Node node;

    private Node getNode() {
        return node;
    }

    private void setNode(Node node) {
        this.node = node;
    }

    public SyncRequestHandler(Node node) {
        this.setNode(node);
    }

    @Override
    public Message handle(ChannelHandlerContext channelHandlerContext, Message message) {
        SyncRequest syncRequest = (SyncRequest) message;
        InetSocketAddress localAddress = ((NioSocketChannel) channelHandlerContext.channel()).localAddress();
        InetSocketAddress remoteAddress = ((NioSocketChannel) channelHandlerContext.channel()).remoteAddress();

        boolean result = false;

        if (syncRequest.getOperationType().equals(Constants.MESSAGE_TYPE_UPDATE)) {
            LOGGER.info("[SyncRequestHandler] [{}:{}<-->{}:{}] Receive SYNC request. UUID = {}, Message UUID = {}" +
                            ", Operation Type = {}, Application Name = {}, Key = {}, Value = {}, Last Update = {}" +
                            ", Last Update Server Id = {}."
                    , localAddress.getHostString(), localAddress.getPort(), remoteAddress.getHostString()
                    , remoteAddress.getPort(), syncRequest.getUuid(), syncRequest.getMessageUuid()
                    , syncRequest.getOperationType(), syncRequest.getApplicationName(), syncRequest.getKey()
                    , syncRequest.getValue(), SimpleDateFormat.getDateTimeInstance().format(syncRequest.getLastUpdate())
                    , syncRequest.getLastUpdateServerId());
            UpdateMessage updateMessage = new UpdateMessage(syncRequest.getMessageUuid(), syncRequest.getApplicationName()
                    , syncRequest.getKey(), syncRequest.getValue(), syncRequest.getLastUpdate(), syncRequest.getLastUpdateServerId());
            result = this.getNode().propagateMessage(updateMessage);
        } else if (syncRequest.getOperationType().equals(Constants.MESSAGE_TYPE_DELETE)) {
            LOGGER.info("[SyncRequestHandler] [{}:{}<-->{}:{}] Receive SYNC request. UUID = {}, Message UUID = {}" +
                            ", Operation Type = {}, Application Name = {}, Key = {}, Last Update = {}" +
                            ", Last Update Server Id = {}."
                    , localAddress.getHostString(), localAddress.getPort(), remoteAddress.getHostString()
                    , remoteAddress.getPort(), syncRequest.getUuid(), syncRequest.getMessageUuid()
                    , syncRequest.getOperationType(), syncRequest.getApplicationName(), syncRequest.getKey()
                    , SimpleDateFormat.getDateTimeInstance().format(syncRequest.getLastUpdate()), syncRequest.getLastUpdateServerId());
            DeleteMessage deleteMessage = new DeleteMessage(syncRequest.getMessageUuid(), syncRequest.getApplicationName()
                    , syncRequest.getKey(), syncRequest.getLastUpdate(), syncRequest.getLastUpdateServerId());
            result = this.getNode().propagateMessage(deleteMessage);
        }

        SyncResponse syncResponse = new SyncResponse();
        syncResponse.setUuid(syncRequest.getUuid());
        syncResponse.setSuccess(result);
        LOGGER.info("[SyncRequestHandler] [{}:{}<-->{}:{}] Send SYNC response. UUID = {}, Success = {}."
                , localAddress.getHostString(), localAddress.getPort(), remoteAddress.getHostString()
                , remoteAddress.getPort(), syncResponse.getUuid(), syncResponse.isSuccess());
        return syncResponse;
    }
}