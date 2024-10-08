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
import ripple.common.entity.Constants;
import ripple.common.helper.TypeHelper;
import ripple.common.tcp.Decoder;
import ripple.common.tcp.Message;
import ripple.common.tcp.MessageType;
import ripple.common.tcp.message.GetResponse;
import ripple.common.tcp.message.GetResponseItem;

import java.util.ArrayList;
import java.util.Date;

/**
 * @author Zhen Tang
 */
public class GetResponseDecoder implements Decoder {
    @Override
    public Message decode(ByteBuf byteBuf, MessageType messageType) {
        GetResponse getResponse = new GetResponse();
        getResponse.setUuid(TypeHelper.readUuid(byteBuf));
        getResponse.setApplicationName(TypeHelper.readString(byteBuf));
        getResponse.setKey(TypeHelper.readString(byteBuf));
        getResponse.setValue(TypeHelper.readString(byteBuf));
        getResponse.setItems(new ArrayList<>());
        int itemsCount = byteBuf.readInt();
        int i = 0;
        for (i = 0; i < itemsCount; i++) {
            GetResponseItem getResponseItem = new GetResponseItem();
            getResponseItem.setMessageUuid(TypeHelper.readUuid(byteBuf));
            getResponseItem.setOperationType(TypeHelper.readString(byteBuf));
            getResponseItem.setApplicationName(TypeHelper.readString(byteBuf));
            getResponseItem.setKey(TypeHelper.readString(byteBuf));
            if (getResponseItem.getOperationType().equals(Constants.MESSAGE_TYPE_UPDATE)) {
                getResponseItem.setValue(TypeHelper.readString(byteBuf));
            } else if (getResponseItem.getOperationType().equals(Constants.MESSAGE_TYPE_INCREMENTAL_UPDATE)) {
                getResponseItem.setBaseMessageUuid(TypeHelper.readUuid(byteBuf));
                getResponseItem.setAtomicOperation(TypeHelper.readString(byteBuf));
                getResponseItem.setValue(TypeHelper.readString(byteBuf));
            }
            getResponseItem.setLastUpdate(new Date(byteBuf.readLong()));
            getResponseItem.setLastUpdateServerId(byteBuf.readInt());
            getResponse.getItems().add(getResponseItem);
        }
        return getResponse;
    }
}