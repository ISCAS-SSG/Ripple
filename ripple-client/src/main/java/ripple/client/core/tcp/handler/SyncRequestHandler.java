// Copyright (c) 2023 Institute of Software, Chinese Academy of Sciences
// Ripple is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ripple.client.core.tcp.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ripple.client.RippleClient;
import ripple.common.entity.AbstractMessage;
import ripple.common.entity.Constants;
import ripple.common.entity.DeleteMessage;
import ripple.common.entity.IncrementalUpdateMessage;
import ripple.common.entity.Item;
import ripple.common.entity.UpdateMessage;
import ripple.common.tcp.Handler;
import ripple.common.tcp.Message;
import ripple.common.tcp.message.Result;
import ripple.common.tcp.message.SyncRequest;
import ripple.common.tcp.message.SyncResponse;

import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Zhen Tang
 */
public class SyncRequestHandler implements Handler {
    // For logging
    public static long StartTime;
    private static final Logger LOGGER = LoggerFactory.getLogger(SyncRequestHandler.class);
    private RippleClient rippleClient;

    public RippleClient getRippleClient() {
        return rippleClient;
    }

    public void setRippleClient(RippleClient rippleClient) {
        this.rippleClient = rippleClient;
    }

    public SyncRequestHandler(RippleClient rippleClient) {
        this.setRippleClient(rippleClient);
    }

    private Result applyMessage(AbstractMessage message) {
        String applicationName = message.getApplicationName();
        String key = message.getKey();
        Item item = this.getRippleClient().getStorage().getItemService().getItem(applicationName, key);
        if (item == null) {
            this.getRippleClient().getStorage().getItemService().newItem(applicationName, key);
        }
        if (!this.getRippleClient().getStorage().getMessageService().exist(message.getUuid())) {
            this.getRippleClient().getStorage().getMessageService().newMessage(message);
            return Result.SUCCESS;
        } else {
            return Result.DUPLICATED;
        }
    }

    @Override
    public Message handle(ChannelHandlerContext channelHandlerContext, Message message) {
        SyncRequest syncRequest = (SyncRequest) message;
        InetSocketAddress localAddress = ((NioSocketChannel) channelHandlerContext.channel()).localAddress();
        InetSocketAddress remoteAddress = ((NioSocketChannel) channelHandlerContext.channel()).remoteAddress();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        // For logging
        try {
            boolean loadTestEnabled = true;
            if (loadTestEnabled) {
                long endTime = System.currentTimeMillis();
                long startTime = syncRequest.getLastUpdate().getTime();

                String clientId = this.getRippleClient().getStorage().getLocation().replace(".sqlite", "");
                System.out.println("[" + clientId + "] " + (endTime - startTime) + " ms. Message received: "
                        + syncRequest.getApplicationName() + "." + syncRequest.getKey() + "=" + syncRequest.getValue());
//                long endTime = System.currentTimeMillis();
//                String[] source = syncRequest.getValue().split(" ");
//                long startTime = Long.parseLong(source[2]);
//                System.out.println("[" + simpleDateFormat.format(new Date(System.currentTimeMillis()))
//                        + "] Received: " + source[0] + "," + source[1] + "," + (endTime - startTime) + "ms. From SYNC.");
            }
        } catch (NumberFormatException ignored) {

        }

        AbstractMessage msg = null;
        if (syncRequest.getOperationType().equals(Constants.MESSAGE_TYPE_UPDATE)) {
            LOGGER.info("[" + simpleDateFormat.format(new Date(System.currentTimeMillis()))
                            + "] [SyncRequestHandler] [{}:{}<-->{}:{}] Receive SYNC request. UUID = {}, Message UUID = {}" +
                            ", Operation Type = {}, Application Name = {}, Key = {}, Value = {}, Last Update = {}" +
                            ", Last Update Server Id = {}."
                    , localAddress.getHostString(), localAddress.getPort(), remoteAddress.getHostString()
                    , remoteAddress.getPort(), syncRequest.getUuid(), syncRequest.getMessageUuid()
                    , syncRequest.getOperationType(), syncRequest.getApplicationName(), syncRequest.getKey()
                    , syncRequest.getValue(), SimpleDateFormat.getDateTimeInstance().format(syncRequest.getLastUpdate())
                    , syncRequest.getLastUpdateServerId());
            msg = new UpdateMessage(syncRequest.getMessageUuid(), syncRequest.getApplicationName()
                    , syncRequest.getKey(), syncRequest.getValue(), syncRequest.getLastUpdate(), syncRequest.getLastUpdateServerId());
        } else if (syncRequest.getOperationType().equals(Constants.MESSAGE_TYPE_DELETE)) {
            LOGGER.info("[" + simpleDateFormat.format(new Date(System.currentTimeMillis()))
                            + "] [SyncRequestHandler] [{}:{}<-->{}:{}] Receive SYNC request. UUID = {}, Message UUID = {}" +
                            ", Operation Type = {}, Application Name = {}, Key = {}, Last Update = {}" +
                            ", Last Update Server Id = {}."
                    , localAddress.getHostString(), localAddress.getPort(), remoteAddress.getHostString()
                    , remoteAddress.getPort(), syncRequest.getUuid(), syncRequest.getMessageUuid()
                    , syncRequest.getOperationType(), syncRequest.getApplicationName(), syncRequest.getKey()
                    , SimpleDateFormat.getDateTimeInstance().format(syncRequest.getLastUpdate()), syncRequest.getLastUpdateServerId());
            msg = new DeleteMessage(syncRequest.getMessageUuid(), syncRequest.getApplicationName()
                    , syncRequest.getKey(), syncRequest.getLastUpdate(), syncRequest.getLastUpdateServerId());
        } else if (syncRequest.getOperationType().equals(Constants.MESSAGE_TYPE_INCREMENTAL_UPDATE)) {
            LOGGER.info("[" + simpleDateFormat.format(new Date(System.currentTimeMillis()))
                            + "] SyncRequestHandler] [{}:{}<-->{}:{}] Receive SYNC request. UUID = {}, Message UUID = {}" +
                            ", Operation Type = {}, Application Name = {}, Key = {}, Base Message UUID = {}" +
                            ", Atomic Operation = {}, Value = {}, Last Update = {}, Last Update Server Id = {}."
                    , localAddress.getHostString(), localAddress.getPort(), remoteAddress.getHostString()
                    , remoteAddress.getPort(), syncRequest.getUuid(), syncRequest.getMessageUuid()
                    , syncRequest.getOperationType(), syncRequest.getApplicationName(), syncRequest.getKey()
                    , syncRequest.getBaseMessageUuid(), syncRequest.getAtomicOperation(), syncRequest.getValue()
                    , SimpleDateFormat.getDateTimeInstance().format(syncRequest.getLastUpdate()), syncRequest.getLastUpdateServerId());
            msg = new IncrementalUpdateMessage(syncRequest.getMessageUuid(), syncRequest.getApplicationName()
                    , syncRequest.getKey(), syncRequest.getBaseMessageUuid(), syncRequest.getAtomicOperation()
                    , syncRequest.getValue(), syncRequest.getLastUpdate(), syncRequest.getLastUpdateServerId());
        }

        Result result = this.applyMessage(msg);

        SyncResponse syncResponse = new SyncResponse();
        syncResponse.setUuid(syncRequest.getUuid());
        syncResponse.setResult(result);
        LOGGER.info("[SyncRequestHandler] [{}:{}<-->{}:{}] Send SYNC response. UUID = {}, Result = {}."
                , localAddress.getHostString(), localAddress.getPort(), remoteAddress.getHostString()
                , remoteAddress.getPort(), syncResponse.getUuid(), syncResponse.getResult());
        return syncResponse;
    }
}