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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ripple.client.RippleClient;
import ripple.client.helper.Api;
import ripple.common.entity.AbstractMessage;
import ripple.common.entity.ClientMetadata;
import ripple.common.entity.Constants;
import ripple.common.entity.DeleteMessage;
import ripple.common.entity.IncrementalUpdateMessage;
import ripple.common.entity.Item;
import ripple.common.entity.UpdateMessage;
import ripple.common.tcp.Handler;
import ripple.common.tcp.Message;
import ripple.common.tcp.message.DispatchRequest;
import ripple.common.tcp.message.DispatchResponse;

import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DispatchRequestHandler implements Handler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DispatchRequestHandler.class);
    private RippleClient rippleClient;

    public RippleClient getRippleClient() {
        return rippleClient;
    }

    public void setRippleClient(RippleClient rippleClient) {
        this.rippleClient = rippleClient;
    }

    public DispatchRequestHandler(RippleClient rippleClient) {
        this.setRippleClient(rippleClient);
    }

    private void applyMessage(AbstractMessage message) {
        String applicationName = message.getApplicationName();
        String key = message.getKey();
        Item item = this.getRippleClient().getStorage().getItemService().getItem(applicationName, key);
        if (item == null) {
            this.getRippleClient().getStorage().getItemService().newItem(applicationName, key);
        }
        if (!this.getRippleClient().getStorage().getMessageService().exist(message.getUuid())) {
            this.getRippleClient().getStorage().getMessageService().newMessage(message);
        }
    }

    @Override
    public Message handle(ChannelHandlerContext channelHandlerContext, Message message) {
        DispatchRequest dispatchRequest = (DispatchRequest) message;
        InetSocketAddress localAddress = ((NioSocketChannel) channelHandlerContext.channel()).localAddress();
        InetSocketAddress remoteAddress = ((NioSocketChannel) channelHandlerContext.channel()).remoteAddress();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        // For logging
        try {
            boolean loadTestEnabled = true;
            if (loadTestEnabled) {
                long endTime = System.currentTimeMillis();
                long startTime = dispatchRequest.getLastUpdate().getTime();
                System.out.println((endTime - startTime) + " ms");
//                String[] source = dispatchRequest.getValue().split(" ");
//                long startTime = Long.parseLong(source[2]);
//                System.out.println("[" + simpleDateFormat.format(new Date(System.currentTimeMillis()))
//                        + "] Received: " + source[0] + "," + source[1] + "," + (endTime - startTime) + "ms. From DISPATCH.");
            }
        } catch (NumberFormatException ignored) {

        }


        if (dispatchRequest.getOperationType().equals(Constants.MESSAGE_TYPE_UPDATE)) {
            LOGGER.info("[DispatchRequestHandler] [{}:{}<-->{}:{}] Receive DISPATCH request. UUID = {}, Client List Signature = {}, Message UUID = {}" +
                            ", Operation Type = {}, Application Name = {}, Key = {}, Value = {}, Last Update = {}" +
                            ", Last Update Server Id = {}."
                    , localAddress.getHostString(), localAddress.getPort(), remoteAddress.getHostString()
                    , remoteAddress.getPort(), dispatchRequest.getUuid(), dispatchRequest.getClientListSignature(), dispatchRequest.getMessageUuid()
                    , dispatchRequest.getOperationType(), dispatchRequest.getApplicationName(), dispatchRequest.getKey()
                    , dispatchRequest.getValue(), SimpleDateFormat.getDateTimeInstance().format(dispatchRequest.getLastUpdate())
                    , dispatchRequest.getLastUpdateServerId());
            AbstractMessage msg = new UpdateMessage(dispatchRequest.getMessageUuid(), dispatchRequest.getApplicationName()
                    , dispatchRequest.getKey(), dispatchRequest.getValue(), dispatchRequest.getLastUpdate(), dispatchRequest.getLastUpdateServerId());
            this.applyMessage(msg);
            this.dispatchMessage(dispatchRequest, msg);
        } else if (dispatchRequest.getOperationType().equals(Constants.MESSAGE_TYPE_DELETE)) {
            LOGGER.info("[DispatchRequestHandler] [{}:{}<-->{}:{}] Receive DISPATCH request. UUID = {}, Client List Signature = {}, Message UUID = {}" +
                            ", Operation Type = {}, Application Name = {}, Key = {}, Last Update = {}" +
                            ", Last Update Server Id = {}."
                    , localAddress.getHostString(), localAddress.getPort(), remoteAddress.getHostString()
                    , remoteAddress.getPort(), dispatchRequest.getUuid(), dispatchRequest.getClientListSignature(), dispatchRequest.getMessageUuid()
                    , dispatchRequest.getOperationType(), dispatchRequest.getApplicationName(), dispatchRequest.getKey()
                    , SimpleDateFormat.getDateTimeInstance().format(dispatchRequest.getLastUpdate()), dispatchRequest.getLastUpdateServerId());
            AbstractMessage msg = new DeleteMessage(dispatchRequest.getMessageUuid(), dispatchRequest.getApplicationName()
                    , dispatchRequest.getKey(), dispatchRequest.getLastUpdate(), dispatchRequest.getLastUpdateServerId());
            this.applyMessage(msg);
            this.dispatchMessage(dispatchRequest, msg);
        } else if (dispatchRequest.getOperationType().equals(Constants.MESSAGE_TYPE_INCREMENTAL_UPDATE)) {
            LOGGER.info("[DispatchRequestHandler] [{}:{}<-->{}:{}] Receive DISPATCH request. UUID = {}, Client List Signature = {}, Message UUID = {}" +
                            ", Operation Type = {}, Application Name = {}, Key = {}, Base Message UUID = {}" +
                            ", Atomic Operation = {}, Value = {}, Last Update = {}, Last Update Server Id = {}."
                    , localAddress.getHostString(), localAddress.getPort(), remoteAddress.getHostString()
                    , remoteAddress.getPort(), dispatchRequest.getUuid(), dispatchRequest.getClientListSignature(), dispatchRequest.getMessageUuid()
                    , dispatchRequest.getOperationType(), dispatchRequest.getApplicationName(), dispatchRequest.getKey()
                    , dispatchRequest.getBaseMessageUuid(), dispatchRequest.getAtomicOperation(), dispatchRequest.getValue()
                    , SimpleDateFormat.getDateTimeInstance().format(dispatchRequest.getLastUpdate()), dispatchRequest.getLastUpdateServerId());
            AbstractMessage msg = new IncrementalUpdateMessage(dispatchRequest.getMessageUuid(), dispatchRequest.getApplicationName()
                    , dispatchRequest.getKey(), dispatchRequest.getBaseMessageUuid(), dispatchRequest.getAtomicOperation()
                    , dispatchRequest.getValue(), dispatchRequest.getLastUpdate(), dispatchRequest.getLastUpdateServerId());
            this.applyMessage(msg);
            this.dispatchMessage(dispatchRequest, msg);
        }

        DispatchResponse dispatchResponse = new DispatchResponse();
        dispatchResponse.setUuid(dispatchRequest.getUuid());
        dispatchResponse.setSuccess(true);
        LOGGER.info("[DispatchRequestHandler] [{}:{}<-->{}:{}] Send DISPATCH response. UUID = {}, Success = {}."
                , localAddress.getHostString(), localAddress.getPort(), remoteAddress.getHostString()
                , remoteAddress.getPort(), dispatchResponse.getUuid(), dispatchResponse.isSuccess());
        return dispatchResponse;
    }

    private void dispatchMessage(DispatchRequest dispatchRequest, AbstractMessage msg) {
        // Get client list and dispatch messages
        List<ClientMetadata> clientList = this.getRippleClient().getClientListCache().get(dispatchRequest.getClientListSignature());
        if (clientList == null) {
            Channel channel = this.getRippleClient().findOrConnectToServer(dispatchRequest.getApplicationName(), dispatchRequest.getKey());
            Api.getClientListAsync(channel, dispatchRequest.getClientListSignature());
            // Double check
            if (this.getRippleClient().getPendingMessages().get(dispatchRequest.getClientListSignature()) == null) {
                synchronized (this.getRippleClient().getPendingMessages()) {
                    if (this.getRippleClient().getPendingMessages().get(dispatchRequest.getClientListSignature()) == null) {
                        this.getRippleClient().getPendingMessages().put(dispatchRequest.getClientListSignature(), new ConcurrentLinkedQueue<>());
                    }
                }
            }
            this.getRippleClient().getPendingMessages().get(dispatchRequest.getClientListSignature()).offer(msg);
        } else {
            ExecutorService pool = Executors.newFixedThreadPool(clientList.size());
            for (ClientMetadata clientMetadata : clientList) {
                Callable<Void> task = new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        Channel channel = getRippleClient().findOrConnectToClient(clientMetadata);
                        Api.syncAsync(channel, msg);
                        return null;
                    }
                };
                pool.submit(task);
            }
            pool.shutdown();
        }
    }
}
