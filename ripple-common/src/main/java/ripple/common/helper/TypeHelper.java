// Copyright (c) 2023 Institute of Software, Chinese Academy of Sciences
// Ripple is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ripple.common.helper;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * @author Zhen Tang
 */
public final class TypeHelper {
    private TypeHelper() {

    }

    public static UUID readUuid(ByteBuf byteBuf) {
        return UUID.fromString(TypeHelper.readString(byteBuf));
    }

    public static void writeUuid(UUID value, ByteBuf byteBuf) {
        TypeHelper.writeString(value.toString(), byteBuf);
    }

    public static String readString(ByteBuf byteBuf) {
        int size = byteBuf.readInt();
        byte[] bytes = new byte[size];
        byteBuf.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static void writeString(String value, ByteBuf byteBuf) {
        if (value == null) {
            throw new NullPointerException("value");
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        byteBuf.writeInt(bytes.length);
        byteBuf.writeBytes(bytes);
    }

    public static boolean readBoolean(ByteBuf byteBuf) {
        byte value = byteBuf.readByte();
        return value == 1 ? true : false;
    }

    public static void writeBoolean(boolean value, ByteBuf byteBuf) {
        byteBuf.writeByte(value ? (byte) 1 : (byte) 0);
    }
}
