package ripple.client.core.tcp.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ripple.client.RippleClient;
import ripple.common.entity.ClientMetadata;
import ripple.common.tcp.Handler;
import ripple.common.tcp.Message;
import ripple.common.tcp.message.GetClientListResponse;
import ripple.common.tcp.message.GetClientListResponseItem;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Zhen Tang
 */
public class GetClientListResponseHandler implements Handler {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetClientListResponseHandler.class);
    private RippleClient rippleClient;

    public RippleClient getRippleClient() {
        return rippleClient;
    }

    public void setRippleClient(RippleClient rippleClient) {
        this.rippleClient = rippleClient;
    }

    public GetClientListResponseHandler(RippleClient rippleClient) {
        this.setRippleClient(rippleClient);
    }

    @Override
    public Message handle(ChannelHandlerContext channelHandlerContext, Message message) {
        GetClientListResponse getClientListResponse = (GetClientListResponse) message;
        InetSocketAddress localAddress = ((NioSocketChannel) channelHandlerContext.channel()).localAddress();
        InetSocketAddress remoteAddress = ((NioSocketChannel) channelHandlerContext.channel()).remoteAddress();
        LOGGER.info("[GetClientListResponseHandler] [{}:{}<-->{}:{}] Receive GET_CLIENT_LIST response. UUID = {}, Client List Signature = {}."
                , localAddress.getHostString(), localAddress.getPort(), remoteAddress.getHostString()
                , remoteAddress.getPort(), getClientListResponse.getUuid(), getClientListResponse.getClientListSignature());

        List<ClientMetadata> clientList = new ArrayList<>();

        for (GetClientListResponseItem getClientListResponseItem : getClientListResponse.getItems()) {
            LOGGER.info("[GetClientListResponseHandler] [{}:{}<-->{}:{}] ---> Client Metadata: Address = {}, Port = {}."
                    , localAddress.getHostString(), localAddress.getPort(), remoteAddress.getHostString()
                    , remoteAddress.getPort(), getClientListResponseItem.getAddress(), getClientListResponseItem.getPort());
            clientList.add(new ClientMetadata(getClientListResponseItem.getAddress(), getClientListResponseItem.getPort()));
        }

        this.getRippleClient().getClientListCache().put(getClientListResponse.getClientListSignature(), clientList);
        return null;
    }
}