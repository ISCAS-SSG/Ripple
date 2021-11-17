package ripple.common.tcp;

public enum MessageType {
//    STRING((byte) -1),
//    STRING_RESPONSE((byte) -2),
//    EMPTY((byte) 0),
//    ACK_REQUEST((byte) 1),
//    ACK_RESPONSE((byte) 2),
//    HEARTBEAT_REQUEST((byte) 3),
//    HEARTBEAT_RESPONSE((byte) 4),
//    SYNC_REQUEST((byte) 5),
//    SYNC_RESPONSE((byte) 6);

    REQUEST((byte) 1), RESPONSE((byte) 2), PING((byte) 3), PONG((byte) 4), EMPTY((byte) 5);

    private byte value;

    public byte getValue() {
        return value;
    }

    private void setValue(byte value) {
        this.value = value;
    }

    MessageType(byte value) {
        this.setValue(value);
    }

    public static MessageType get(byte type) {
        for (MessageType elem : values()) {
            if (elem.getValue() == type) {
                return elem;
            }
        }
        throw new RuntimeException("Unsupported type: " + type);
    }
}
