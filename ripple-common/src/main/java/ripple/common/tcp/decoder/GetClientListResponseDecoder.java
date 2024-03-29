// Copyright (c) 2023 Institute of Software, Chinese Academy of Sciences
// Ripple is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ripple.common.tcp.decoder;

import io.netty.buffer.ByteBuf;
import ripple.common.helper.TypeHelper;
import ripple.common.tcp.Decoder;
import ripple.common.tcp.Message;
import ripple.common.tcp.MessageType;
import ripple.common.tcp.message.GetClientListResponse;
import ripple.common.tcp.message.GetClientListResponseItem;

import java.util.ArrayList;

/**
 * @author Zhen Tang
 */
public class GetClientListResponseDecoder implements Decoder {
    @Override
    public Message decode(ByteBuf byteBuf, MessageType messageType) {
        GetClientListResponse getClientListResponse = new GetClientListResponse();
        getClientListResponse.setUuid(TypeHelper.readUuid(byteBuf));
        getClientListResponse.setClientListSignature(TypeHelper.readString(byteBuf));
        getClientListResponse.setItems(new ArrayList<>());
        int itemsCount = byteBuf.readInt();
        int i = 0;
        for (i = 0; i < itemsCount; i++) {
            GetClientListResponseItem getClientListResponseItem = new GetClientListResponseItem();
            getClientListResponseItem.setAddress(TypeHelper.readString(byteBuf));
            getClientListResponseItem.setPort(byteBuf.readInt());
            getClientListResponse.getItems().add(getClientListResponseItem);
        }
        return getClientListResponse;
    }
}